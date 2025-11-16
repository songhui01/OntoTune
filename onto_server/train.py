import storage
import model
import os
import shutil
import reg_blocker
import json
import sampler
import copy

class OntoTrainingException(Exception):
    pass

def train_and_swap(fn, old, tmp, verbose=False):
    
    if os.path.exists(fn):
        old_model = model.OntoRegression(have_cache_data=True)
        old_model.load(fn)
    else:
        old_model = None

    new_model = train_and_save_model(tmp, verbose=verbose)
    max_retries = 5
    current_retry = 1
    while not reg_blocker.should_replace_model(old_model, new_model):
        if current_retry >= max_retries == 0:
            print("Could not train model with better regression profile.")
            return
        
        print("New model rejected when compared with old model. "
              + "Trying to retrain with emphasis on regressions.")
        print("Retry #", current_retry)
        new_model = train_and_save_model(tmp, verbose=verbose,
                                         emphasize_experiments=current_retry)
        current_retry += 1

    if os.path.exists(fn):
        shutil.rmtree(old, ignore_errors=True)
        os.rename(fn, old)
    os.rename(tmp, fn)

def train_and_save_model(fn, verbose=True, emphasize_experiments=0):
    BUDGET = int(os.getenv("ONTO_BUDGET", "1000"))
    TEMPLATE_CAP = int(os.getenv("ONTO_TEMPLATE_CAP", "100"))
    ARM_MIN = int(os.getenv("ONTO_ARM_MIN", "5"))
    HARD_RATIO = float(os.getenv("ONTO_HARD_RATIO", "0.20"))

    all_experience = storage.experience()

    for _ in range(emphasize_experiments):
        all_experience.extend(storage.experiment_experience())
    
    def _tpl_get(it):
        return it.meta.get("template_id", None)

    all_experience = sampler.select_samples_budgeted(
        all_experience,
        num_arms=6,
        budget=BUDGET,
        per_template_cap=TEMPLATE_CAP,
        arm_min_coverage=ARM_MIN,
        hard_tail_ratio=HARD_RATIO,
        template_getter=_tpl_get
    )
    
    x = []
    for j, r in all_experience:
        obj = json.loads(j)
        if "Plan" in obj and "metadata" in obj and "arm_config" in obj:
            x.append(obj)
        elif "Plan" in obj:
            x.append({
                "Plan": obj["Plan"],
                "metadata": obj.get("metadata", {}),
                "arm_config": obj.get("arm_config", {}),
            })
        else:
            x.append({
                "Plan": obj,
                "metadata": obj.get("metadata", {}),
                "arm_config": obj.get("arm_config", {}),
            })

    y = [i[1] for i in all_experience]        
    
    if not all_experience:
        raise OntoTrainingException("Cannot train a Onto model with no experience")
    
    if len(all_experience) < 20:
        print("Warning: trying to train a Onto model with fewer than 20 datapoints.")

    reg = model.OntoRegression(have_cache_data=True, verbose=verbose)
    reg.fit(x, y)
    reg.save(fn)
    return reg


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: train.py MODEL_FILE")
        exit(-1)
    train_and_save_model(sys.argv[1])

    print("Model saved, attempting load...")
    reg = model.OntoRegression(have_cache_data=True)
    reg.load(sys.argv[1])

