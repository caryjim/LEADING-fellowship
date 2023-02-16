#For RAMP 2019 Dataset

import os

os.getcwd()
os.chdir("D:/Documents/PycharmProjects/LEADING-RAMP")
print("Current working directory: {0}".format(os.getcwd()))



msu_pc_data, msu_ai_data = process_repo("montana_state_university")
