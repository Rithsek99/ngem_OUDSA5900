import subprocess

def run_python_script(file_path):
    process = subprocess.Popen(['python', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f'Running {file_path}...')
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        print(f'Error occurred while running {file_path}:')
        print(stderr.decode('utf-8'))
    else:
        print(f'Successfully executed {file_path}:')
        print(stdout.decode('utf-8'))

def main():
    python_files_to_run = ['peruwebscrapev2.py', 'createdb_from_csv.py', 'query.py']

    for file_path in python_files_to_run:
        run_python_script(file_path)

if __name__ == '__main__':
    main()
