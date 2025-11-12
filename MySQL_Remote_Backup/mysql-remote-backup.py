import os
import paramiko
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

class DatabaseBackup:
    def __init__(self):
        # SSH connection details
        self.ssh_host = os.getenv('SSH_HOST')
        self.ssh_port = int(os.getenv('SSH_PORT', 22))
        self.ssh_user = os.getenv('SSH_USER')
        self.ssh_key_path = os.getenv('SSH_KEY_PATH')
        
        # Database details
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '3306')
        self.db_name = os.getenv('DB_NAME')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        
        # Backup settings
        self.remote_backup_dir = os.getenv('REMOTE_BACKUP_DIR', '/tmp')
        self.local_backup_dir = os.getenv('LOCAL_BACKUP_DIR', './backups')
        
        # Create local backup directory if it doesn't exist
        Path(self.local_backup_dir).mkdir(parents=True, exist_ok=True)
        
    def create_ssh_client(self):
        """Create and return an SSH client connection"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            # Connect using SSH key
            if not self.ssh_key_path or not os.path.exists(self.ssh_key_path):
                raise ValueError(f"SSH key not found at: {self.ssh_key_path}")
            
            client.connect(
                hostname=self.ssh_host,
                port=self.ssh_port,
                username=self.ssh_user,
                key_filename=self.ssh_key_path
            )
            return client
        except Exception as e:
            print(f"Error connecting to SSH server: {e}")
            raise
    
    def generate_backup_filename(self):
        """Generate a timestamped backup filename"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{self.db_name}_backup_{timestamp}.sql"
    
    def create_backup_command(self, backup_file):
        """Generate the MySQL backup command"""
        remote_path = f"{self.remote_backup_dir}/{backup_file}"
        
        cmd = f"mysqldump -h {self.db_host} -P {self.db_port} -u {self.db_user} "
        if self.db_password:
            cmd += f"-p'{self.db_password}' "
        cmd += f"{self.db_name} > {remote_path}"
        
        return cmd, remote_path
    
    def backup(self):
        """Execute the backup process"""
        backup_file = self.generate_backup_filename()
        local_path = os.path.join(self.local_backup_dir, backup_file)
        
        print(f"Starting backup of database '{self.db_name}'...")
        print(f"Connecting to {self.ssh_host}...")
        
        ssh_client = None
        sftp_client = None
        
        try:
            # Create SSH connection
            ssh_client = self.create_ssh_client()
            print("SSH connection established.")
            
            # Generate and execute backup command
            backup_cmd, remote_path = self.create_backup_command(backup_file)
            print(f"Executing backup command on remote host...")
            
            stdin, stdout, stderr = ssh_client.exec_command(backup_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error_msg = stderr.read().decode()
                raise Exception(f"Backup command failed: {error_msg}")
            
            print(f"Backup created successfully on remote host: {remote_path}")
            
            # Download the backup file
            print(f"Downloading backup to {local_path}...")
            sftp_client = ssh_client.open_sftp()
            sftp_client.get(remote_path, local_path)
            print(f"Backup downloaded successfully!")
            
            # Optional: Remove remote backup file after download
            remove_remote = os.getenv('REMOVE_REMOTE_BACKUP', 'true').lower() == 'true'
            if remove_remote:
                sftp_client.remove(remote_path)
                print(f"Remote backup file removed.")
            
            # Get file size
            file_size = os.path.getsize(local_path)
            print(f"Backup size: {file_size / (1024*1024):.2f} MB")
            print(f"Backup completed successfully: {local_path}")
            
            return local_path
            
        except Exception as e:
            print(f"Error during backup: {e}")
            raise
        finally:
            if sftp_client:
                sftp_client.close()
            if ssh_client:
                ssh_client.close()

if __name__ == "__main__":
    try:
        backup = DatabaseBackup()
        backup.backup()
    except Exception as e:
        print(f"Backup failed: {e}")
        exit(1)