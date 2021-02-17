import os
import glob
import shutil


def files_from_directory(src_dir, regex="*.*"):
    """
    return a list of all files in src_dir that match the regex
    """
    filename_regex = os.path.join(src_dir, regex)
    filelist = glob.glob(filename_regex)
    return filelist


def move_file(filepath, outdir):
    filename = os.path.basename(filepath)
    output_path = os.path.join(outdir, filename)
    shutil.move(filepath, output_path)
