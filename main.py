'''This project is used to evaluate the quality of the sequencing reads from NGS
It was build to work with forward and reverse reads. Working with threads:
    1) run FATSQC and MULTIQC,
    2) run PEAR to merge forward and reverse and remove low quality sequences,
    3) run FATSQC and MULTIQC after the merge,
'''

import os, concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import fastqc, quality

MAX_WORKERS = 3


def run_quality_control_fastqc(path, files, output_path, assembled_files):

    task_description = fastqc.create_task(files, path, output_path, assembled_files) #create jobs
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(fastqc.process_task, task) for task in task_description] #submit jobs in parallel

        for f in concurrent.futures.as_completed(tasks):
            result = f.result()
            print(result)
        fastqc.run_multiqc(output_path) #runs multiqc, when all files were analyzed by fastqc


def run_merge_remove_low_quality(path, files):
    task_description = quality.create_task(files, path)
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(quality.process_task, task) for task in task_description]

        for f in concurrent.futures.as_completed(tasks):
            result = f.result()
            print(result)


if __name__ == '__main__':
    '''Folder were the fastqc files are located'''
    root_path = '/Users/flavia/Documents/NGS_project/2nd-half/'
    files = os.listdir(root_path)

    '''NGS Demutiplex Data Fastqc Analysis'''
    output_path = os.path.join(root_path, "1-Report-QC") #folder_name to add the output files
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    run_quality_control_fastqc(root_path, files, output_path, assembled_files=False)

    '''Merged Paired Reads and Quality Control'''
    output_path = os.path.join(root_path, "2-Merge-QC") #folder_name to add the output files
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    run_merge_remove_low_quality(root_path, files)

    '''Paired Reads Fastqc Analysis'''
    output_path = os.path.join(root_path, "3-Report-QC") #folder_name to add the output files
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    path = os.path.join(root_path, '2-Merge-QC')
    files = os.listdir(path)
    run_quality_control_fastqc(path, files, output_path, assembled_files=True)

