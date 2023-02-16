# From Jon
import pandas as pd
from zipfile import ZipFile
import glob

def extract_subset_ramp_data(zip_file, ir_repo_id):
    """This function tries to conserve memory by opening zipped RAMP monthly
       data files one at a time and subsetting the data to a single repository's
       data for that month, prior to further processing or aggregation.

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
        A Pandas dataframe. The subset of RAMP data for the specified
        repository and month.

    """
    with ZipFile(zip_file) as rampzip:
        with rampzip.open(rampzip.namelist()[0]) as rampfile:
            ramp_df = pd.read_csv(rampfile)
    ir_data = ramp_df[ramp_df["repository_id"] == ir_repo_id].copy()
    return ir_data

#-------------------------------------------------------------------------------------

def get_ir_data(ir_repo_id, cols, file_list):
    """This function iterates through a list of zipped RAMP data files to
       aggregate a subset of complete RAMP data for a single IR. Creates
       an empty Pandas dataframe and then appends monthly data to it.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the
        repository whose data will be aggregated.

    cols:
        List. Column names to be used in the empty dataframe. Appended data will
        have the same columns by default.

    file_list:
        List. File paths to monthly RAMP data in zipped format.

    Returns
    -------

    ir_data:
        A Pandas dataframe. The aggregated RAMP data for the specified IR
        across all months included in the file_list.

    """
    ir_data = pd.DataFrame(columns=cols)
    # append data from each month
    for mo_data in file_list:
        print(mo_data)
        ir_data = ir_data.append(extract_subset_ramp_data(mo_data, ir_repo_id))
    return ir_data

#------------------------------------------------------------------------------------------------

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
        String. A locally unique repository identifier, designating the
        repository whose data will be aggregated.

    Returns
    -------

    ir_v1_data:
        A Pandas dataframe. The aggregated RAMP "v1" data for the specified IR
        across all months included in the all_data_file_list.
    """
    all_cols = ['citableContent', 'clickThrough', 'clicks', 'country', 'date',
                'device', 'impressions', 'index', 'position', 'url',
                'repository_id']
    all_data_file_list = glob.glob("/content/drive/MyDrive/00_ramp/*all.zip")
    ir_v1_data = get_ir_data(ir_repo_id, all_cols, all_data_file_list)
    return ir_v1_data

#----------------------------------------------------------------------------------------------------
def get_v2_pc_data(ir_repo_id):
    """This function aggregates all RAMP page click data harvested for a single
       IR since August 19, 2018 ("v2" data). Column names for those data are
       hard coded. A list of data files is also generated.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the
        repository whose data will be aggregated.

    Returns
    -------

    ir_v2_pc_data:
        A Pandas dataframe. The aggregated RAMP page clicks data for the
        specified IR across all months included in the pageclick_data_file_list.
    """
    pageclick_cols = ['citableContent', 'clickThrough', 'clicks', 'date',
                      'impressions', 'index', 'position', 'url',
                      'repository_id']
    pageclick_data_file_list = glob.glob("/content/drive/MyDrive/00_ramp/*all_page-clicks.zip")
    ir_v2_pc_data = get_ir_data(ir_repo_id, pageclick_cols,
                                pageclick_data_file_list)
    return ir_v2_pc_data

#----------------------------------------------------------------------------------------------------------
def get_v2_ai_data(ir_repo_id):
    """This function aggregates all RAMP country/device access data harvested
       for a single  IR since August 19, 2018 ("v2" data). Column names for
       those data are hard coded. A list of data files is also generated.

    Parameters
    ----------

    ir_repo_id:
        String. A locally unique repository identifier, designating the
        repository whose data will be aggregated.

    Returns
    -------

    ir_v2_ai_data:
        A Pandas dataframe. The aggregated RAMP country/device data for the
        specified IR across all months included in the
        demographic_data_file_list.
    """
    demographic_cols = ['clickThrough', 'clicks', 'country', 'date', 'device',
                        'impressions', 'index', 'position', 'repository_id']
    demographic_data_file_list = glob.glob("/content/drive/MyDrive/00_ramp/*all_country-device-info.zip")
    ir_v2_ai_data = get_ir_data(ir_repo_id, demographic_cols,
                                demographic_data_file_list)
    return ir_v2_ai_data

#----------------------------------------------------------------------------------------------------------

def concat_ramp_versions(v1_data, v2_data):
    subset_cols = list(v2_data.columns.values)
    v1_subset = v1_data[subset_cols].copy()
    v1_v2_concatenated = pd.concat([v1_subset, v2_data], ignore_index=True)
    return v1_v2_concatenated

#-----------------------------------------------------------------------------------------------------------

def process_repo(ir_repo_id):
    """This is basically a workflow function that calls all the other functions.

    Parameters
    ----------

    ir_repo_id:
       String. A locally unique identifier for the repository whose data will
       be aggregated.

    Returns
    -------

    ir_complete_pc_data:
       A Pandas dataframe. The aggregated page click data across all the
       years/months specified for the specified repository.

    ir_complete_ai_data:
       A Pandas dataframe. The aggreated country/device data across all the
       years/months specified for the specified repository.

    """
    ir_v1_data = get_v1_data(ir_repo_id)
    ir_v2_pc_data = get_v2_pc_data(ir_repo_id)
    ir_v2_ai_data = get_v2_ai_data(ir_repo_id)
    ir_complete_pc_data = concat_ramp_versions(ir_v1_data, ir_v2_pc_data)
    ir_complete_ai_data = concat_ramp_versions(ir_v1_data, ir_v2_ai_data)
    return ir_complete_pc_data, ir_complete_ai_data