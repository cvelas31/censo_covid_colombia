import os


def create_dict_files(directory):
    """
    Creates a dictionary with the filepath of each table for each department

    Parameters:
    -----------
    directory : os.path
        Directory of the censo data

    Return:
    -------
    dict_paths_departments : dict
        Dictioanry of each deparrtment with the corresponding filepath for each table
    """
    dict_paths_departments = {}
    for name in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, name)):
            curr_path = os.path.join(directory, name, name + "_CSV")
            current_dict_files = {}
            for c_file in os.listdir(curr_path):
                if "VIV" in c_file:
                    current_dict_files["VIV"] = os.path.join(curr_path, c_file)
                elif "HOG" in c_file:
                    current_dict_files["HOG"] = os.path.join(curr_path, c_file)
                elif "PER" in c_file:
                    current_dict_files["PER"] = os.path.join(curr_path, c_file)
                elif "FALL" in c_file:
                    current_dict_files["FALL"] = os.path.join(curr_path, c_file)
                elif "MGN" in c_file:
                    current_dict_files["MGN"] = os.path.join(curr_path, c_file)
            dict_paths_departments[name[3:]] = current_dict_files
    return dict_paths_departments
