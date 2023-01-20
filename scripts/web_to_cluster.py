import os
import subprocess
import paramiko
import wget


student_number = "s3049221"
year_to_download_from = "2013"
months = list(range(1, 13))
types = [{"prefix": "RC", "type": "comments"}, {"prefix": "RS", "type": "submissions"}]


## For the code to work, you need the zstd tool downloaded from https://sourceforge.net/projects/zstandard.mirror/
## Once downloaded, have to add it to the path
## You also need to create a password.txt file in this folder that contains your password to your local machine on the cluster

if __name__ == '__main__':
    for type in types:
        for i in months:
            month = str(i) if i >= 10 else f"0{i}"
            file_no_ext = f"{type['prefix']}_{year_to_download_from}-{month}"
            file_zst = f"{file_no_ext}.zst"
            file_json = f"{file_no_ext}.json"
            if not os.path.exists(os.path.join(os.getcwd(), file_zst)):
                link = f"https://files.pushshift.io/reddit/{type['type']}/{file_zst}"
                wget.download(link, file_zst)
            else:
                print(f"{file_zst} already exists")
            if not os.path.exists(os.path.join(os.getcwd(), file_json)):
                command = f"zstd -d {file_zst} --long=31 -o {file_json}"
                process = subprocess.Popen(command, shell=True)
                return_code = process.wait()
                print(file_json, return_code)
            else:
                print(f"{file_json} already exists")
            with open("password.txt", "rt") as f:
                pwd = f.read()
            command = f"pscp -P 22 -pw {pwd} .\\{file_json} {student_number}@ctit022.ewi.utwente.nl:"
            powershell_command = f'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe {command}'
            process = subprocess.Popen(powershell_command, shell=True)
            return_code = process.wait()
            print(file_json, return_code)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ssh.connect("ctit022.ewi.utwente.nl", username=f"{student_number}", password=pwd)
                ssh.exec_command(f"hdfs dfs -copyFromLocal /home/{student_number}/{file_json} /user/{student_number}/reddit/{file_json}")
            except TimeoutError:
                print("TimeoutError. Please do something")
                input("Have you done something? [input]")
                ssh.connect("ctit022.ewi.utwente.nl", username=f"{student_number}", password=pwd)
                ssh.exec_command(
                    f"hdfs dfs -copyFromLocal /home/{student_number}/{file_json} /user/{student_number}/reddit/{file_json}")
            os.remove(file_zst)
            os.remove(file_json)