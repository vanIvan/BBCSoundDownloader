import csv
import os
import re
import sys
import urllib.request
import zipfile
from multiprocessing.pool import ThreadPool
from pathlib import Path

THREAD_COUNT = 10
MAX_FILENAME_LENGTH = 143  # limit for ecryptfs, 255 on most normal file systems

class Downloader:
    def __init__(self, thread_count=THREAD_COUNT):
        self.thread_count = thread_count
        self.samples = self.get_samples()
        self.total_count = len(self.samples)
        self.finished = 0
        self.failed = 0

    def get_samples(self):
        """
        Reads BBCSoundEffects.csv and returns a list of (url, final_wav_path).
        Note that we now form the URL for the zipped WAV file, and plan to extract it.
        """
        samples = []
        
        csv_path = os.path.join(os.path.dirname(__file__), 'BBCSoundEffects.csv')
        with open(csv_path, encoding='utf8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Example row['location']: "NHU05094029.wav"
                # We want something like:
                #    https://sound-effects-media.bbcrewind.co.uk/zip/NHU05094029.wav.zip?download&rename=BBC_...
                
                # Create a folder name from the CDName
                folder = self.sanitize_path(row['CDName'])
                
                # We'll store the final file with .wav extension
                wav_suffix = '.wav'
                max_description_length = MAX_FILENAME_LENGTH - len(wav_suffix)
                
                # Use 'description' as the base of your filename
                description = self.sanitize_path(row['description'])[:max_description_length]
                filename = description + wav_suffix
                final_wav_path = Path('sounds') / row['location']
                
                # If it already exists, skip
                if final_wav_path.exists():
                    continue
                
                # Build the download URL: We assume row['location'] is something like "NHU05094029.wav"
                # We'll tack on '.zip' plus '?download&rename=...' 
                # so the final is e.g. "https://sound-effects-media.bbcrewind.co.uk/zip/NHU05094029.wav.zip?download&rename=BBC_description.wav"
                
                # Clean up the rename piece, removing spaces/odd chars
                rename_part = "BBC_" + folder + "_" + description + ".wav"
                rename_part = self.sanitize_path(rename_part)
                
                # The base part of row['location']: e.g. "NHU05094029.wav"
                # We'll just append ".zip" and then "?download&rename=..."
                # So if row['location'] == "NHU05094029.wav",
                # final piece is "NHU05094029.wav.zip?download&rename=..."
                location_zip = row['location'] + ".zip"
                
                url = (
                    "https://sound-effects-media.bbcrewind.co.uk/zip/"
                    + location_zip
                    + "?download&rename="
                    + row['location']
                )
                
                samples.append((url, final_wav_path))
        
        return samples

    def download_all(self):
        """
        Kick off threaded downloads/unzips for all samples.
        """
        print(f"Downloading {self.total_count} samples using {self.thread_count} threads...")
        results = ThreadPool(self.thread_count).map(self.download, self.samples)
        
        print("\nExecution completed. Reporting any failures:")
        for success, filepath, exc in results:
            if not success:
                print(f"{filepath} failed with exception: {exc}")
        print(f"{self.failed} failures reported.")

    def download(self, sample):
        """
        Downloads the zip from the URL, extracts the WAV within, and renames it to the final path.
        """
        url, final_wav_path = sample
        print(f"Starting download: {url}\n -> {final_wav_path}")
        
        try:
            # Ensure parent folder structure exists
            final_wav_path.parent.mkdir(parents=True, exist_ok=True)

            # 1) Download the zip file to a temp file
            temp_zip_path, _ = urllib.request.urlretrieve(url)
            
            # 2) Extract the WAV from the ZIP
            #    (We assume the zip has exactly one WAV inside, or at least we want the first .wav.)
            with zipfile.ZipFile(temp_zip_path, 'r') as zf:
                # For safety, pick the first .wav inside
                wav_members = [m for m in zf.namelist() if m.lower().endswith('.wav')]
                if not wav_members:
                    raise RuntimeError("No WAV found in downloaded zip")
                
                # Extract the first .wav from the zip
                extracted_wav_name = zf.extract(wav_members[0], path=final_wav_path.parent)
                
            # 3) Move/rename the extracted WAV to final_wav_path
            extracted_wav_path = Path(extracted_wav_name)
            extracted_wav_path.rename(final_wav_path)
            
            self.finished += 1
            print(f"({self.finished}/{self.total_count}) Finished {final_wav_path}")
            return (True, None, None)
        
        except Exception as e:
            self.failed += 1
            print("FAILED: " + str(final_wav_path), file=sys.stderr)
            print(str(e), file=sys.stderr)
            print(f"{self.failed} failed download attempts", file=sys.stderr)
            return (False, final_wav_path, e)

    def sanitize_path(self, path_str):
        """
        Replaces all non-alphanumeric or common punctuation chars with '_'.
        Also trims leading/trailing whitespace.
        """
        # Keep alphanumerics, underscores, hyphens, ampersand, commas, parentheses, periods, and spaces
        return re.sub(r'[^\w\-&,()\. ]', '_', path_str).strip()


if __name__ == "__main__":
    Downloader().download_all()
