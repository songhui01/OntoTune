
import torch
import torch.nn as nn

class CNNMatrixDelta(nn.Module):
    def __init__(self, in_channels, num_arms, hidden_channels=(64,128,128,64), kernel_size=5, dilations=(1,2,4,8), dropout=0.1):
        super().__init__()
        self.in_channels = in_channels
        self.num_arms = num_arms
        layers = []
        ch = in_channels
        dil = list(dilations) + [1]*max(0, (len(hidden_channels)-len(dilations)))
        for h, d in zip(hidden_channels, dil):
            pad = (kernel_size - 1) // 2 * d
            layers += [nn.Conv1d(ch, h, kernel_size=kernel_size, dilation=d, padding=pad),
                       nn.LeakyReLU(),
                       nn.Dropout(dropout)]
            ch = h
        self.backbone = nn.Sequential(*layers)
        feat_dim = ch*2
        self.base_head = nn.Sequential(
            nn.Linear(feat_dim, 256),
            nn.LeakyReLU(),
            nn.Linear(256, 64),
            nn.LeakyReLU(),
            nn.Linear(64, 1),
        )
        self.delta_head = nn.Sequential(
            nn.Linear(feat_dim, 256),
            nn.LeakyReLU(),
            nn.Linear(256, self.num_arms),
        )
        for m in self.delta_head[-1:].modules():
            if isinstance(m, nn.Linear):
                nn.init.zeros_(m.weight)
                nn.init.zeros_(m.bias)

    def encode(self, x):
        if x.dim() == 2:
            x = x.transpose(0,1).unsqueeze(0)
        elif x.dim() == 3:
            x = x.transpose(1,2)
        else:
            raise ValueError(f"Unexpected input shape: {tuple(x.shape)}")
        z = self.backbone(x)
        z_max = torch.amax(z, dim=2)
        z_avg = torch.mean(z, dim=2)
        g = torch.cat([z_max, z_avg], dim=1)
        return g

    def forward(self, x):
        g = self.encode(x)
        base = self.base_head(g).squeeze(-1)
        delta = self.delta_head(g)
        return base, delta
