import argparse
import socket
import json

def __json_bytes(obj):
    return (json.dumps(obj) + "\n").encode("UTF-8")

def __send_json(s, obj):
    b = json.dumps(obj).encode("utf-8")
    s.sendall(len(b).to_bytes(4, byteorder='big'))
    s.sendall(b)
    
def __connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 9381))
    return s

def send_model_load(path):
    with __connect() as s:
        __send_json(s, {"type": "load model"})
        __send_json(s, {"path": path})
        __send_json(s, {"final": True})

        try:
            ack = s.recv(4)
            print("[INFO] Got ack from server:", ack)
        except:
            print("[WARN] No ack received.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Onto for PostgreSQL Controller")
    parser.add_argument("--load",
                        metavar="PATH",
                        help="Load the saved Onto model")
    parser.add_argument("--train",
                        metavar="PATH",
                        help="Train a Onto model and save it")
    parser.add_argument("--retrain", action="store_true",
                        help="Force the Onto server to train a model and load it")
    parser.add_argument("--test-connection", action="store_true",
                        help="Test the connection from the Onto server to the PostgreSQL instance.")
    parser.add_argument("--add-test-query", metavar="PATH",
                        help="Add the SQL query in the file at PATH to the test query list")
    parser.add_argument("--status", action="store_true",
                        help="Print out information about the Onto server.")
    parser.add_argument("--experiment", metavar="SECONDS", type=int,
                        help="Conduct experiments on test queries for (up to) SECONDS seconds.")
    
    args = parser.parse_args()

    if args.train:
        import train
        print("Training Onto model from collected experience")
        train.train_and_save_model(args.train)
        exit(0)

    if args.load:
        import model
        print("Attempting to load the Onto model...")
        reg = model.OntoRegression(have_cache_data=True)
        reg.load(args.load)
        
        print("Model loaded. Sending message to Onto server...")
        send_model_load(args.load)
        print("Message sent to server.")
        exit(0)

    if args.retrain:
        import train
        from constants import DEFAULT_MODEL_PATH, OLD_MODEL_PATH, TMP_MODEL_PATH
        train.train_and_swap(DEFAULT_MODEL_PATH, OLD_MODEL_PATH, TMP_MODEL_PATH,
                             verbose=True)
        send_model_load(DEFAULT_MODEL_PATH)
        exit(0)

    if args.test_connection:
        from reg_blocker import ExperimentRunner
        er = ExperimentRunner()
        if er.test_connection():
            print("Connection successful!")
            exit(0)
        else:
            print("Could not connect to PostgreSQL.")
            exit(1)
        
    if args.add_test_query:
        from reg_blocker import ExperimentRunner
        er = ExperimentRunner()

        with open(args.add_test_query) as f:
            sql = f.read()
        
        er.add_experimental_query(sql)
        exit(0)

    if args.experiment:
        from reg_blocker import ExperimentRunner
        er = ExperimentRunner()
        er.explore(args.experiment)
        exit(0)

    if args.status:
        from reg_blocker import ExperimentRunner
        er = ExperimentRunner()
        info = er.status()

        max_key_length = max(len(x) for x in info.keys())

        for k, v in info.items():
            print(k.ljust(max_key_length), ":", v)
            
        exit(0)


    
