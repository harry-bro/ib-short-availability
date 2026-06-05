#!/usr/bin/env python3
"""
FTP monitor utility for IB shortstock feed.

Extracted from monitoring.py for reusability in the ETL pipeline.

Note: erroneous deletions can be found in logs when script accesses FTP during an update.
"""

from ftplib import FTP
import json
from pathlib import Path
import socket


class FTPMonitor:
    """
    Poll an FTP directory for .md5 file changes.
    
    Detects NEW / UPDATED / DELETED base files by hashing the md5 contents.
    Persists last-seen md5s in a JSON state file.
    """
    
    def __init__(self, host, user, directory='/', check_interval=60,
                 state_file=None, timeout=60):
        """
        Args:
            host: FTP hostname.
            user: FTP username (passwordless for this feed).
            directory: FTP working directory to scan.
            check_interval: target seconds between checks (wall clock).
            state_file: path to persisted md5 map (json). 
            timeout: socket timeout for FTP operations.
        """
        self.host = host
        self.user = user
        self.directory = directory
        self.check_interval = check_interval
        self.timeout = timeout
        self.state_file = state_file or "ftp_monitor_state.json"
        self.known_md5s = self.load_state()
    
    def load_state(self):
        """Load previously seen MD5 hashes from JSON; return {} if absent."""
        state_path = Path(self.state_file)
        if state_path.exists():
            with open(state_path, 'r') as file:
                return json.load(file)
        return {}
    
    def save_state(self):
        """Write current MD5 mapping to JSON."""
        state_path = Path(self.state_file)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(self.known_md5s, indent=2))
    
    def get_md5_files(self):
        """
        Fetch all .md5 files from FTP and return {filename: md5hex}.
        
        Raises:
            RuntimeError: On FTP connection error
        """
        md5_contents = {}
        try:
            with FTP(timeout=self.timeout) as ftp:
                ftp.connect(self.host, 21, timeout=self.timeout)
                ftp.login(user=self.user)
                ftp.cwd(self.directory)
                files = ftp.nlst()
                
                for md5_file in (f for f in files if f.endswith('.md5')):
                    lines = []
                    ftp.retrlines(f'RETR {md5_file}', lines.append)
                    if not lines:
                        continue
                    first = " ".join(lines).strip().split()
                    if not first:
                        continue
                    md5_hash = first[0]
                    if len(md5_hash) >= 32:  # coarse sanity check
                        md5_contents[md5_file] = md5_hash.lower()
        except (OSError, socket.timeout) as e:
            raise RuntimeError(f"FTP connection error: {e}")
        
        return md5_contents
    
    def check_changes(self):
        """
        Compare current .md5 map to persisted state.
        
        Returns:
            List of tuples: [('new'|'updated'|'deleted', filename, md5_or_none), ...]
        """
        current_md5s = self.get_md5_files()
        changes = []

        # New and updated files
        for filename, md5_hash in current_md5s.items():
            if filename not in self.known_md5s:
                changes.append(('new', filename, md5_hash))
            elif self.known_md5s[filename] != md5_hash:
                changes.append(('updated', filename, md5_hash))
        
        # Deleted files
        for filename in self.known_md5s:
            if filename not in current_md5s:
                changes.append(('deleted', filename, None))

        # Persist only when something changed
        if changes:
            self.known_md5s = current_md5s
            self.save_state()
        
        return changes