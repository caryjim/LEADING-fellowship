from aggregation_helpers import *


def get_global_daily_clicks():
    all_data_file_list = glob.glob("./ramp_zipped/*/*all.zip")
    pageclick_data_file_list = glob.glob("./ramp_zipped/*/*all_page-clicks.zip")
    demographic_data_file_list = glob.glob("./ramp_zipped/*/*all_country-device-info.zip")
    day_pc_clicks_df, day_ai_clicks_df = process_global_daily_clicks(all_data_file_list,
                                                                     pageclick_data_file_list,
                                                                     demographic_data_file_list)
    day_pc_clicks_df.to_csv("RAMP_complete_daily_pc_clicks.csv", index=False)
    day_ai_clicks_df.to_csv("RAMP_complete_daily_ai_clicks.csv", index=False)
    return


def get_per_ir_daily_clicks():
    ir_info = pd.read_csv("RAMP_repository_info.csv")
    for ir in sorted(ir_info["repository_id"]):
        print(ir)
        ir_pc_data, ir_ai_data = process_repo_day_clicks(ir)
        ir_pc_data.to_csv("daily_clicks/" + ir + "_RAMP_pc_daily_clicks.csv", index=False)
        ir_ai_data.to_csv("daily_clicks/" + ir + "_RAMP_ai_daily_clicks.csv", index=False)
    return


# Uncomment below to get global RAMP daily clicksums and save to file
# get_global_daily_clicks()


# Uncomment below to get per-IR daily clicksums and save to file
# get_per_ir_daily_clicks()