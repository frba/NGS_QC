import os


def run_multiqc(output_path):
    os.system(f'multiqc {output_path} -o {output_path}')
    return f'Finished Multiqc analysis'


def process_task(task_description):
    task_id, file_path, output_path = task_description
    os.system(f'fastqc --noextract --nogroup -o {output_path} {file_path}')
    return f'Finished task {task_id}'


def create_task(files, path, output_path, assembled_files):
    task_id = 1
    for file in files:
        if assembled_files:
            if file.__contains__('.assembled.fastq'):
                file_path = os.path.join(path, file)
                task_description = [task_id, file_path, output_path]
                task_id+=1
                yield task_description
        else:
            if file.endswith('fastq.gz'):
                file_path = os.path.join(path, file)
                task_description = [task_id, file_path, output_path]
                task_id += 1
                yield task_description



