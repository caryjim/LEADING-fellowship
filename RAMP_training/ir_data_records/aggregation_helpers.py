import pandas as pd
from zipfile import ZipFile
import glob


def extract_daily_pc_clicks(zip_file):
    """Function for processing zip files to conserve memory and space. Reads in a
    file of RAMP CSV data and subsets to two columns, date and clicks. Used for
    global RAMP data - does not filter on specific IR.

    :param zip_file:
        A zip file containing a CSV of RAMP data.

    :return daily_clicks:
        A pandas dataframe subset to date and clicks columns.
    """
    with ZipFile(zip_file) as rampzip:
        with rampzip.open(rampzip.namelist()[0]) as rampfile:
            ramp_df = pd.read_csv(rampfile)
    daily_clicks = pd.DataFrame(columns=["date", "clicks"])
    for name, group in ramp_df.groupby("date"):
        daily_clicks = daily_clicks.append(pd.DataFrame([[name, group["clicks"].sum()]], columns=["date", "clicks"]))
    return daily_clicks


def extract_daily_ai_clicks(zip_file):
    """Function for processing zip files to conserve memory and space. Reads in a
    file of RAMP CSV data and subsets to four columns, date, country, device. and clicks.
    Used for global RAMP data - does not filter on specific IR.

    :param zip_file:
        A zip file containing a CSV of RAMP data.

    :return daily_clicks:
        A pandas dataframe subset to date, country, device. and clicks columns.
    """
    with ZipFile(zip_file) as rampzip:
        with rampzip.open(rampzip.namelist()[0]) as rampfile:
            ramp_df = pd.read_csv(rampfile)
    cols = ["date", "country", "device", "clicks"]
    daily_clicks = pd.DataFrame(columns=cols)
    for name, group in ramp_df.groupby(["date", "country", "device"]):
        daily_clicks = daily_clicks.append(pd.DataFrame([[name[0], name[1], name[2], group["clicks"].sum()]],
                                                        columns=cols))
    return daily_clicks


def extract_subset_ramp_data(zip_file, ir_repo_id):
    """This function tries to conserve memory by opening zipped RAMP monthly data
       files one at a time and subsetting the data to a single repository's data
       for that month, prior to further processing or aggregation.

    Parameters
    ----------

    zip_file:
        String. A file path pointing to a zip file.

    ir_repo_id:
        String. A locally unique repository identifier which will be used to
        subset the unzipped data.

    Returns
    -------

    ir_data:
        A Pandas dataframe. The subset of RAMP data for the specified repository and month.

    """
    with ZipFile(zip_file) as rampzip:
        with rampzip.open(rampzip.namelist()[0]) as rampfile:
            ramp_df = pd.read_csv(rampfile)
    ir_data = ramp_df[ramp_df["repository_id"] == ir_repo_id].copy()
    return ir_data


def get_ir_data(ir_repo_id, cols, file_list):
    """This function iterates through a list of zipped RAMP data files to
       aggregate a subset of complete RAMP data for a single IR. Creates
       an empty Pandas dataframe and then appends monthly data to it.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the repository
        whose data will be aggregated.

    cols:
        List. Column names to be used in the empty dataframe. Appended data will
        have the same columns by default.

    file_list:
        List. File paths to monthly RAMP data in zipped format.

    Returns
    -------

    ir_data:
        A Pandas dataframe. The aggregated RAMP data for the specified IR across all
        months included in the file_list.

    """
    ir_data = pd.DataFrame(columns=cols)
    # append data from each month
    for mo_data in file_list:
        # print(mo_data)
        ir_data = ir_data.append(extract_subset_ramp_data(mo_data, ir_repo_id))
    return ir_data


def get_v1_data(ir_repo_id):
    """This function aggregates all RAMP data that was harvested for a single IR
       between January 1, 2017 and August 18, 2018 ("v1" data). Column names
       for those data are hard coded. A list of data files is also generated.

       TODO: It may be useful to remove this function and subset the v1
       data for each month as part of the v2 aggregations. Leaving it for
       now in case we want the option to store/save v1 data by itself.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the repository
        whose data will be aggregated.

    Returns
    -------

    ir_v1_data:
        A Pandas dataframe. The aggregated RAMP "v1" data for the specified IR across all
        months included in the all_data_file_list.
    """
    all_cols = ['citableContent', 'clickThrough', 'clicks', 'country', 'date', 'device',
                'impressions', 'index', 'position', 'url', 'repository_id']
    all_data_file_list = glob.glob("./ramp_zipped/*/*all.zip")
    ir_v1_data = get_ir_data(ir_repo_id, all_cols, all_data_file_list)
    return ir_v1_data


def get_v2_pc_data(ir_repo_id):
    """This function aggregates all RAMP page click data harvested for a single
       IR since August 19, 2018 ("v2" data). Column names for those data are hard
       coded. A list of data files is also generated.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the repository
        whose data will be aggregated.

    Returns
    -------

    ir_v2_pc_data:
        A Pandas dataframe. The aggregated RAMP page clicks data for the specified
        IR across all months included in the pageclick_data_file_list.
    """
    pageclick_cols = ['citableContent', 'clickThrough', 'clicks', 'date', 'impressions',
                      'index', 'position', 'url', 'repository_id']
    pageclick_data_file_list = glob.glob("./ramp_zipped/*/*all_page-clicks.zip")
    ir_v2_pc_data = get_ir_data(ir_repo_id, pageclick_cols, pageclick_data_file_list)
    return ir_v2_pc_data


def get_v2_ai_data(ir_repo_id):
    """This function aggregates all RAMP country/device access data harvested for
       a single  IR since August 19, 2018 ("v2" data). Column names for those data
       are hard coded. A list of data files is also generated.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the repository
        whose data will be aggregated.

    Returns
    -------

    ir_v2_ai_data:
        A Pandas dataframe. The aggregated RAMP country/device data for the specified
        IR across all months included in the demographic_data_file_list.
    """
    demographic_cols = ['clickThrough', 'clicks', 'country', 'date', 'device', 'impressions',
                        'index', 'position', 'repository_id']
    demographic_data_file_list = glob.glob("./ramp_zipped/*/*all_country-device-info.zip")
    ir_v2_ai_data = get_ir_data(ir_repo_id, demographic_cols, demographic_data_file_list)
    return ir_v2_ai_data


def concat_ramp_versions(v1_data, v2_data):
    subset_cols = list(v2_data.columns.values)
    v1_subset = v1_data[subset_cols].copy()
    v1_v2_concatenated = pd.concat([v1_subset, v2_data], ignore_index=True)
    return v1_v2_concatenated


def process_repo(ir_repo_id):
    """This is basically a workflow function that calls all the other functions.

    Parameters
    ----------

    ir_repo_id:
       String. A locally unique identifier for the repository whose data will be aggregated.

    Returns
    -------

    ir_complete_pc_data:
       A Pandas dataframe. The aggregated page click data across all the years/months
       specified for the specified repository.

    ir_complete_ai_data:
       A Pandas dataframe. The aggreated country/device data across all the years/months
       specified for the specified repository.

    """
    ir_v1_data = get_v1_data(ir_repo_id)
    ir_v2_pc_data = get_v2_pc_data(ir_repo_id)
    ir_v2_ai_data = get_v2_ai_data(ir_repo_id)
    ir_complete_pc_data = concat_ramp_versions(ir_v1_data, ir_v2_pc_data)
    ir_complete_ai_data = concat_ramp_versions(ir_v1_data, ir_v2_ai_data)
    return ir_complete_pc_data, ir_complete_ai_data


def process_repo_day_clicks(ir_repo_id):
    """This is basically a workflow function that calls all the other functions.
       Similar to the above function, but aggregates daily click data. For page click
       data the aggregation is per IR per day. For access info date the aggregation
       is per IR per combination of date, country, device.

    Parameters
    ----------

    ir_repo_id:
       String. A locally unique identifier for the repository whose data will be aggregated.

    Returns
    -------

    daily_pc_clicks:
       A Pandas dataframe. The aggregated daily page click clicksums across all the years/months
       specified for the specified repository.

    daily_ai_clicks:
       A Pandas dataframe. The aggregated daily country/device clicksums across all the years/months
       specified for the specified repository.
    """
    ir_v1_data = get_v1_data(ir_repo_id)
    ir_v2_pc_data = get_v2_pc_data(ir_repo_id)
    ir_v2_ai_data = get_v2_ai_data(ir_repo_id)
    ir_complete_pc_data = concat_ramp_versions(ir_v1_data, ir_v2_pc_data)
    ir_complete_ai_data = concat_ramp_versions(ir_v1_data, ir_v2_ai_data)
    ir_complete_pc_data_day_clicks = ir_complete_pc_data.groupby("date")
    daily_pc_clicks = pd.DataFrame(columns=["date", "clicks", "repository_id"])
    for name, group in ir_complete_pc_data_day_clicks:
        daily_pc_clicks = daily_pc_clicks.append(pd.DataFrame([[name, group["clicks"].sum(), ir_repo_id]],
                                                              columns=["date", "clicks", "repository_id"]))
    ai_cols = ["date", "country", "device", "clicks", "repository_id"]
    daily_ai_clicks = pd.DataFrame(columns=ai_cols)
    for name, group in ir_complete_ai_data.groupby(["date", "country", "device"]):
        daily_ai_clicks = daily_ai_clicks.append(pd.DataFrame([[name[0], name[1], name[2],
                                                                group["clicks"].sum(), ir_repo_id]],
                                                              columns=ai_cols))
    return daily_pc_clicks, daily_ai_clicks


def process_global_daily_clicks(alL_flist, pc_flist, ai_flist):
    """This function reads file names from a list and aggregates
       global RAMP data per day.

    Parameters
    ----------

    alL_flist:
        String. A list of files of RAMP data from before August 19, 2018.

    pc_flist:
        String. A list of files of RAMP page-click data from Aug 19, 2018 onward.

    ai_flist:
        String. A list of RAMP access-info (country-device) data from Aug 19, 2018, onward.

    Returns
    -------

    day_pc_clicks_df:
        A pandas dataframe. Aggregated daily page-click clicksums across the entire period for which
        data are included in the file lists.

    day_ai_clicks_df:
        A pandas dataframe. Aggregated daily access-info clicksums across the entire period for which
        data are included in the file lists.
    """
    all_pc_file_list = alL_flist + pc_flist
    all_ai_file_list = alL_flist + ai_flist

    # Aggregate per day clicksums
    day_pc_clicks_df = pd.DataFrame(columns=["date", "clicks"])
    for f in all_pc_file_list:
        print(f)
        day_pc_clicks_df = day_pc_clicks_df.append(extract_daily_pc_clicks(f))

    # Aggregate per day, country, device combo clicksums
    day_ai_clicks_df = pd.DataFrame(columns=["date", "country", "device", "clicks"])
    for f in all_ai_file_list:
        print(f)
        day_ai_clicks_df = day_ai_clicks_df.append(extract_daily_ai_clicks(f))

    return day_pc_clicks_df, day_ai_clicks_df

