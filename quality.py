import os


def process_task(task_description):
    task_id, quality, minimal_lenght, file_fwd_path, file_rev_path, output_file_path = task_description
    os.system(f'pear -f {file_fwd_path} -r {file_rev_path} -q {quality} -t {minimal_lenght} -o {output_file_path}')
    return f'Finished task {task_id}'


def create_task(files, path):
    task_id = 1
    quality = 25 #minimun value for reads quality
    minimal_lenght = 20 #minimun legth for the reads sequences, please select the best value for your experiment
    for file in files:
        if file.endswith('R1.fastq.gz'): #identify files with forward reads
            output_file_path = os.path.join(path, "2-Merge-QC", file)
            file_fwd_path = os.path.join(path, file)
            file_rev_name = file.replace('_R1.fastq.gz','_R2.fastq.gz') #identify the file with reverse reads,
                                                                        # with same name as forward to do the merge
            file_rev_path = os.path.join(path, file_rev_name)
            task_description = [task_id, quality, minimal_lenght, file_fwd_path, file_rev_path, output_file_path]
            task_id+=1
            yield task_description



