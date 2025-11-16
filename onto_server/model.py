import json
import numpy as np
import collections
import torch
import torch.optim
import joblib
import os
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
import torch.nn as nn

from torch.utils.data import DataLoader
import featurize
from logger import log_matrix, close_log

from net_cnn_delta import CNNMatrixDelta

CUDA = torch.cuda.is_available()

import torch.optim as optim
import torch.nn.functional as F
import math



def _nn_path(base):
    return os.path.join(base, "nn_weights")

def _x_transform_path(base):
    return os.path.join(base, "x_transform")

def _y_transform_path(base):
    return os.path.join(base, "y_transform")

def _channels_path(base):
    return os.path.join(base, "channels")

def _n_path(base):
    return os.path.join(base, "n")


def _inv_log1p(x):
    return np.exp(x) - 1


def _onto_path(base, name):
            return os.path.join(base, f"onto_{name}")

class OntoRegression:
    def __init__(self, have_cache_data=False, verbose=False):
        # self.verbose = verbose
        self.have_cache_data = have_cache_data

        # --- kNN related ---
        self.knn_db_g = None
        self.knn_db_g_norm = None
        self.knn_db_y = None
        self.knn_db_arm = None
        self.knn_k = 5

        self.alpha_temp = 0.06
        self.alpha_min = 0.30
        self.alpha_max = 0.70

        log_transformer = preprocessing.FunctionTransformer(np.log1p, np.expm1, validate=True)
        scale_transformer = preprocessing.MinMaxScaler()

        self.num_trained = 0

        self.num_arms = 6
        self.verbose = verbose
        self.model = None
        self.in_channels = None
        log_t = preprocessing.FunctionTransformer(np.log1p, np.expm1, validate=True)
        self.reward_pipeline = Pipeline([('log', log_t), ('scale', preprocessing.MinMaxScaler())])


    def _ensure_model(self, in_dim, device):
        need_rebuild = (self.model is None) or (getattr(self.model, "in_dim", None) != in_dim)
        if need_rebuild:
            print(f"[OntoRegression] build/rebuild GCN with in_dim={in_dim}")
            self.model = GCNModel(in_dim=in_dim, hidden_dim=64, dropout=0.5)
            self.model.in_dim = in_dim
            self.model.to(device)

    def adjacency_to_edge_index(self, adj):
        row, col = torch.nonzero(adj, as_tuple=True)
        edge_index = torch.stack([row, col], dim=0)
        return edge_index

    def is_torch_model(self):
        return (
            isinstance(self.model, nn.Module)
            and hasattr(self.model, "parameters")
            and len(list(self.model.parameters())) > 0
        )

    def predict(self, plans):
        results = []
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        if self.is_torch_model():
            self.model.to(device)
            self.model.eval()

        num_of_arms = getattr(self, "num_arms", None) or 7

        for plan in plans:
            meta = plan["metadata"]
            arm_cfg = plan.get("arm_config") or plan.get("arm_config_json", {})
            meta["arm_config_json"] = arm_cfg

            arm_idx = int(arm_cfg.get("index", 0))
            if arm_idx < 0 or arm_idx >= num_of_arms:
                arm_idx = 0

            X = featurize.build_feature_matrix(meta, num_of_arms, plan).T

            if self.is_torch_model():
                X = torch.from_numpy(X).float().to(device)
                with torch.no_grad():
                    base, delta = self.model(X)
                    delta = delta.view(-1, num_of_arms)[0]

                    raw   = delta[arm_idx]
                    tau = 0.5
                    k   = 6.0

                    score = F.softplus(raw / tau) - math.log(2.0)

                    y_scaled = torch.sigmoid(k * score)

                    eps = 1e-3
                    y_scaled = torch.clamp(y_scaled - 0.5, 0.0, 1.0)

                    pred_scaled = float(y_scaled.item())

            else:
                X_flat = X.flatten().reshape(1, -1)
                pred_scaled = float(self.model.predict([X_flat]).reshape(-1)[0])

            real_pred = self.reward_pipeline.inverse_transform([[1.0 - pred_scaled]])[0][0]

            results.append(real_pred)

        return np.array(results, dtype=float)


    def fit(self, plans, rewards):
        assert isinstance(plans, (list, tuple)), "fit(plans, rewards): plans must be a list"
        rewards = np.array(rewards).reshape(-1)
        if len(plans) != len(rewards):
            raise ValueError(f"plans ({len(plans)}) and rewards ({len(rewards)}) length mismatch")

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        y = np.array(rewards, dtype=np.float32).reshape(-1, 1)
        y_scaled = (1.0 - self.reward_pipeline.fit_transform(y)).astype(np.float32).squeeze(1)

        X_list, arm_ids, y_list = [], [], []
        for plan, yv in zip(plans, y_scaled): 
            meta = dict(plan.get("metadata", {}) or {})
            arm_idx = meta.get('arm_config_json', {}).get('index', -1)
            if arm_idx < 0 or arm_idx >= self.num_arms:
                continue
            X = featurize.build_feature_matrix(meta, self.num_arms, plan).T 
            X_list.append(torch.tensor(X, dtype=torch.float32))
            arm_ids.append(int(arm_idx))
            y_list.append(float(yv))
        if len(X_list) == 0:
            raise RuntimeError('No samples with valid arm index')

        if self.in_channels is None:
            self.in_channels = X_list[0].shape[1]
        self.model = CNNMatrixDelta(in_channels=self.in_channels, num_arms=self.num_arms).to(device)

        optimizer = optim.AdamW(self.model.parameters(), lr=1e-3, weight_decay=1e-3) 
        mse = nn.MSELoss()

        epochs, min_epoch, patience = 50, 20, 5
        best_loss, patience_ctr = 1e9, 0

        self.model.train()
        for ep in range(epochs):
            total = 0.0
            for i, X in enumerate(X_list):
                X = X.to(device)
                a = arm_ids[i]
                target = torch.tensor(y_list[i], dtype=torch.float32, device=device)

                base, delta = self.model(X)
                base  = base.view(-1)[0]
                delta = delta.view(-1, self.num_arms)[0]

                pred = base + delta[a]
                loss_base  = mse(base,  target)
                loss_delta = mse(delta[a], (target - base).detach())

                loss = loss_base + 0.5 * loss_delta

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                total += float(loss.item())

            if self.verbose and (ep % 5 == 0 or ep == epochs-1):
                print(f"[CNN] epoch {ep} loss={total:.4f}")

            if total + 1e-4 < best_loss:
                best_loss = total
                patience_ctr = 0
            else:
                patience_ctr += 1
                if ep >= min_epoch and patience_ctr >= patience:
                    if self.verbose:
                        print(f"[CNN] early stop at {ep}")
                    break

        self.model.eval()
        return self

    def save(self, path):
        os.makedirs(path, exist_ok=True)
        torch.save(self.model.state_dict(), os.path.join(path, 'onto_cnn_delta.pt'))
        import joblib
        with open(os.path.join(path, 'onto_y_transform'), 'wb') as f:
            joblib.dump(self.reward_pipeline, f)
        with open(os.path.join(path, 'onto_channels'), 'wb') as f:
            joblib.dump(self.in_channels, f)

    def load(self, path):
        import joblib
        with open(os.path.join(path, 'onto_y_transform'), 'rb') as f:
            self.reward_pipeline = joblib.load(f)
        with open(os.path.join(path, 'onto_channels'), 'rb') as f:
            self.in_channels = joblib.load(f)
        self.model = CNNMatrixDelta(in_channels=self.in_channels, num_arms=self.num_arms)
        state = torch.load(os.path.join(path, 'onto_cnn_delta.pt'),
                           map_location=('cuda' if torch.cuda.is_available() else 'cpu'))
        self.model.load_state_dict(state)
        self.model.to(torch.device('cuda' if torch.cuda.is_available() else 'cpu'))
        self.model.eval()
        return self