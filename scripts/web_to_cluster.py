import os
import subprocess
import paramiko


student_number = "s3049221"
year_to_download_from = "2020"


## For the code to work, you need the zstd tool downloaded from https://sourceforge.net/projects/zstandard.mirror/
## Once downloaded, have to add it to the path
## You also need to create a password.txt file in this folder that contains your password to your local machine on the cluster

if __name__ == '__main__':
    for type in [{"prefix": "RC", "type": "comments"}, {"prefix": "RS", "type": "submissions"}]:
        for i in range(1, 13):
            month = str(i) if i >= 10 else f"0{i}"
            file_no_ext = f"{type['prefix']}_{year_to_download_from}-{month}"
            file_zst = f"{file_no_ext}.zst"
            file_json = f"{file_no_ext}.json"
            link = f"https://files.pushshift.io/reddit/{type['type']}/{file_zst}"
            command = f"wget {link} -outfile {file_zst}"
            print(command)
            powershell_command = f'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe {command}'
            process = subprocess.Popen(powershell_command, shell=True)
            return_code = process.wait()
            print(return_code)
            command = f"zstd -d {file_zst} --long=31 -o {file_json}"
            process = subprocess.Popen(command, shell=True)
            return_code = process.wait()
            print(return_code)
            with open("password.txt", "rt") as f:
                pwd = f.read()
            command = f"pscp -P 22 -pw {pwd} .\\{file_json} {student_number}@ctit022.ewi.utwente.nl:"
            powershell_command = f'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe {command}'
            process = subprocess.Popen(command, shell=True)
            return_code = process.wait()
            print(return_code)
            os.remove(file_zst)
            os.remove(file_json)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect("ctit022.ewi.utwente.nl", username=f"{student_number}", password=pwd)
            ssh.exec_command(f"hdfs dfs -copyFromLocal /home/{student_number}/{file_json} /user/{student_number}/reddit/{file_json}")