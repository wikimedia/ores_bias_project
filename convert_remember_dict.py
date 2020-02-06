import fire
import pickle
import pyRemembeR

def run(input, output): 
    py_dict = pickle.load(open(input,'rb'))
    remember = pyRemembeR.Remember(output)
    remember.r = py_dict
    remember.save_to_r()

fire.Fire(run)

