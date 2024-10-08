import os
import logging
import time
import traceback
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from django.conf import settings
from django.utils import timezone
from datetime import timedelta
from watchdog.observers.polling import PollingObserver
from ._base_logger import Logger
from ._base_worker import BaseWorker

from IngestionEngine.models import SourceData

# Setting up logger instance
log = Logger("FileWatcherWorker").get_logger()

# Constants for worker states
WorkerName = "FileWatcherWorker"
InProgress = "InProgress"
Completed = "Completed"
Failed = "Failed"
Scheduled = "Scheduled"
Pause = "Pause"
ReadyForIngestion = "Ready_For_Ingestion"

# List of directories to watch for changes
directories = ["Satellite", "Arial"]

# List of valid file extensions to process
valid_extensions = [
    "tiff",
    "jp2",
    "tif",
    "img",
    "jpg",
    "jpeg",
    "png",
    "geotiff",
    "gtif",
    "nitf",
    "hdf",
    "h5",
]

valid_excel_extension = ["xls", "xlsx", "csv"]

source_data_type = "Hot Folder 1"


class HotFolderEventHandler(FileSystemEventHandler):
    # Processing files that are Copied in the HotFolder.

    # This method is designed to generate Source File Name from Excel File.
    def generate_source_dataset_name(self, image_name):
        # Split the image name by "_" to get the new file name

        log(
            "Reached at the 'generate_source_dataset_name' Method & About to Execute.",
            level=logging.DEBUG,
        )

        try:
            # Split the image name by "_" to separate different parts of the filename
            parts = image_name.split("_")

        except Exception as e:
            log(
                f"'{image_name}' is not Valid Excel File Name"
                f" and File Name should be 'FileName_extension.xlsx/xls' example"
                f" Excel File Name is 'Sample_tiff.xlsx'"
                f" for Sample.tiff Source File. {str(e)}",
                level=logging.ERROR,
            )

        try:
            # Get the file extension from the last part after splitting by "." (assuming the last part contains the file extension)
            file_extension = parts[-1].split(".")[0]

            # log the new file extension
            log(f"New File Extension : {file_extension}", level=logging.DEBUG)

            # Concatenate the new file name and extension to get the new source data name.
            log(
                "Concatenate the new file name and extension to get Source File Name for perticular Excel File.",
                level=logging.DEBUG,
            )

            # Join all parts of the filename except the last one with underscores to create the new file name
            new_file_name = "_".join(parts[:-1])

            # Concatenate the new file name and the file extension to create the new source dataset name
            new_source_data_name = f"{new_file_name}.{file_extension}"

            log(
                f"Source File Name Generated Successfully for Perticular Excel File"
                f" and Source File Name is : {new_source_data_name}.",
                level=logging.DEBUG,
            )
            # Return the new source data path
            return new_source_data_name
        except Exception as e:
            log(
                f"Unexpected Error occured.{str(e)} and {traceback.format_exc()}",
                level=logging.DEBUG,
            )

    # This function is designed to identify all files that have not been tracked by the FileWatcher.
    def untracked_files(self, hot_folder_path):
        # List all file paths in the hot folder and its subdirectories.
        log(
            "List all file paths in the hot folder and its subdirectories.",
            level=logging.DEBUG,
        )
        try:
            hot_folder_files = [
                os.path.join(root, file)
                for root, dirs, files in os.walk(hot_folder_path)
                for file in files
            ]
        except Exception as e:
            log(
                f"There Appears to be an Issue or Difficulty in Tracking the Files within the Hot Folder: {e} and {traceback.format_exc()}",
                level=logging.ERROR,
            )

        # Get all file paths saved in the SourceData table of the database.
        log(
            "Getting all file paths saved in the SourceData table of the database.",
            level=logging.DEBUG,
        )
        try:
            database_files = SourceData.objects.values_list(
                "SourceDatasetPath", flat=True
            )
        except Exception as e:
            log(
                f"Oops!! Got an Error While Retrieving SourceDatasetPath Values from the Database: {e} and {traceback.format_exc()}",
                level=logging.ERROR,
            )

        # Find the files that are in hot folder but not in the database.
        log(
            "Finding files that are in Hot Folder but not in the database.",
            level=logging.DEBUG,
        )
        untracked_files_list = [
            file for file in hot_folder_files if file not in database_files
        ]

        log(f"Untracked files are : {untracked_files_list}.", level=logging.DEBUG)

        return untracked_files_list

    # This function will add all untracked files in the database.
    def add_untracked_file_in_database(self):
        log(
            "Reached at the 'add_untracked_file_in_database' Method & About to Execute.",
            level=logging.INFO,
        )

        # Getting hot folder path
        log(
            "Getting hot folder path.",
            level=logging.INFO,
        )

        hot_folder_path = settings.HOT_FOLDER

        # Callling untracked_files method to get list of all untracked files.
        log(
            "Callling 'untracked_files' method to get list of all Untracked Files.",
            level=logging.DEBUG,
        )

        try:
            untracked_files_list = self.untracked_files(hot_folder_path)
        except Exception as e:
            log(
                f"Oops!! Got an Error While Retrieving List of Untracked Files: {e} and {traceback.format_exc()}",
                level=logging.ERROR,
            )

        log(
            "List of Untracked files Received Successfully.",
            level=logging.DEBUG,
        )

        # From all untracked files filter valid files means files which is having valid file extension.
        filtered_valid_files = [
            file
            for file in untracked_files_list
            if any(file.lower().endswith(f".{ext}") for ext in valid_extensions)
        ]

        log(f"Filtered valid files : {filtered_valid_files}", level=logging.DEBUG)

        log("Checking Each File in Filtered Valid Files List.", level=logging.DEBUG)

        if filtered_valid_files:
            # Checking each valid file path in filtered valid files list
            for file_path in filtered_valid_files:
                # Extract File Name Only from Whole File Path.
                image_name = os.path.basename(file_path)

                # Extract the image type from the directory name.
                image_type = os.path.basename(os.path.dirname(file_path))

                # Get the file size.
                file_size = os.path.getsize(file_path)

                # Create SourceData object.
                created_on = timezone.now()

                # add valid  untracked file in database.
                source_data = SourceData.objects.create(
                    SourceDatasetPath=file_path,
                    SourceDatasetName=image_name,
                    CreatedOn=created_on,
                    SourceDatasetSize=file_size,
                    SourceDatasetType=source_data_type,
                    IsZippedFile=False,
                    IADSIngestionStatus="NA",
                    IADSCoreStatus="NA",
                )

                source_data.save()
                log(
                    "Untracked File Added Successfully in Database.",
                    level=logging.DEBUG,
                    source_data=source_data,
                )

                # get current size of a file.
                new_size  = os.path.getsize(file_path)
                log(f"Initial metadata saved for {file_path}. Monitoring file size...", level=logging.DEBUG)

                # Now, proceed with the logic to monitor file size stabilization.
                log(f"Monitoring file size for stabilization: {file_path}", level=logging.DEBUG)

                # Polling mechanism to check if file size has stabilized.
                size_stable = False

                last_size = new_size
                log(f"Last recorded size of the file: {last_size}", level=logging.DEBUG)

                while not size_stable:
                                time.sleep(10)  # Wait for 10 seconds before checking again
                                current_size = os.path.getsize(file_path)
                                if current_size == last_size:
                                    size_stable = True
                                else:
                                    last_size = current_size
                log(f"File size stabilized for {image_name}. Final size: {current_size}.", level=logging.DEBUG)
                source_data.SourceDatasetSize = current_size
                source_data.save()
                log(
                    f"Updated SourceData with stabilized file size for {file_path}.",
                    level=logging.DEBUG,
                    source_data=source_data,
                )
                previous_size = source_data.SourceDatasetSize
                # calculating difference between previous and new size.
                difference = new_size - previous_size

                if difference <= 0:
                                log("File Uploading Finished.", level=logging.INFO, source_data=source_data)
                                source_data.SourceDatasetSize = new_size
                                source_data.ModifiedOn = timezone.now()
                                source_data.IADSIngestionStatus = ReadyForIngestion
                                source_data.IADSCoreStatus = WorkerName + "_" + Completed
                                source_data.save()
                                log("Source Data Updated Successfully in Database.", level=logging.DEBUG, source_data=source_data)
                                log("Well Done!! FileWatcherWorker Completed Successfully.", level=logging.DEBUG, source_data=source_data)
                else:
                    log("The File Uploading in Progress.", level=logging.INFO, source_data=source_data)
        else:
            log(
                "No Valid Files were Added when FileWatcher was not Active.",
                level=logging.DEBUG,
            )

        # From all untracked files filter all valid excel files means files which is having valid excel file extension.
        filtered_valid_excel_files = [
            file
            for file in untracked_files_list
            if any(file.lower().endswith(f".{ext}") for ext in valid_excel_extension)
        ]

        log(
            f"List of Filtered Valid Excel Files is : {filtered_valid_excel_files}.",
            level=logging.DEBUG,
        )

        if filtered_valid_excel_files:
            # Checking each valid excel file path in filtered valid excel files list.
            for excel_file_path in filtered_valid_excel_files:
                # Extract File Name Only from Whole File Path.
                image_name = os.path.basename(excel_file_path)

                # Created Source File path for Excel File.
                try:
                    # calling generate_source_dataset_name method to generate source file name for excel file path.
                    new_source_data_name = self.generate_source_dataset_name(image_name)

                    # check if source file name is generated or not.
                    if new_source_data_name:
                        log(
                            f"Source File Name for Perticular Excel File Created Successfully."
                            f" and Name is : {new_source_data_name}.",
                            level=logging.INFO,
                        )

                        # Check if source file is present in list of untracked valid files.
                        matching_source_file = [
                            file_path
                            for file_path in filtered_valid_files
                            if file_path.endswith(new_source_data_name)
                        ]

                        # Checking if source file is present in list of untracked valid files.
                        if matching_source_file:
                            log(
                                f"Great!! Source File is present in list of Untracked Valid Files."
                                f" and Source File present at : {matching_source_file}.",
                                level=logging.DEBUG,
                            )
                            try:
                                # Checking if Source Data File exists in Database or not.
                                log(
                                    "Checking if Source Data File exists in Database or not."
                                )
                                source_data = SourceData.objects.get(
                                    SourceDatasetName=new_source_data_name
                                )

                                log(
                                    "Source Data File Exists in Database.",
                                    level=logging.DEBUG,
                                )
                                log(
                                    f"For this Excel file Source File Exists in Hot Folder.and"
                                    f" Source File Path is : {source_data.SourceDatasetPath}.",
                                    level=logging.DEBUG,
                                )

                                # If file path present in database then set IsExcelPresent flag True.
                                log(
                                    "Set IsExcelPresent flag True for Perticular Source File in Database.",
                                    level=logging.DEBUG,
                                )
                                source_data.IsExcelPresent = True
                                source_data.save()

                            except Exception as e:
                                # log the exception.
                                log(
                                    f"For This Excel file Source File Does not Exists: {e} and {traceback.format_exc()}",
                                    level=logging.WARNING,
                                )
                        else:
                            log(
                                f"This '{image_name}' Excel file is already Processed for Source File in HotFolder.",
                                level=logging.WARNING,
                            )

                    else:
                        log(
                            "Source File Name Can't Generated Becuase Excel File Format is not Valid.",
                            level=logging.WARNING,
                        )
                except Exception as e:
                    log(
                        f"Oops!! There might be an error: {e} and {traceback.format_exc()}",
                        level=logging.ERROR,
                    )
        else:
            log(
                "No Valid Excel Files are present in the Hot Folder.",
                level=logging.DEBUG,
            )

    # override on_created event.
    def on_created(self, event):
        log(
            "Reached at the 'on_created' Event & About to Execute.", level=logging.DEBUG
        )
        # Check if the event is not a directory.
        if not event.is_directory:
            log(
                "Obtaining File Path for a Recently Added File within the HotFolder.",
                level=logging.DEBUG,
            )
            # Obtain the file path of the recently added file.
            file_path = event.src_path
            log("Extract File Name Only from Whole File Path.", level=logging.DEBUG)
            # Extract the file name only from the whole file path.
            image_name = os.path.basename(file_path)
            log(f"Extracted File Name is : {image_name}.", level=logging.DEBUG)
            # Extract the image type from the directory name.
            image_type = os.path.basename(os.path.dirname(file_path))
            log(f"File Type is : {image_type}.", level=logging.DEBUG)
            # Extract the Extension of a File.
            image_extension = image_name.split(".")[-1]
            log(f"Image extension is : {image_extension}.", level=logging.DEBUG)
            # Check if the file extension is valid.
            if image_extension in valid_extensions:
                # Our Code is now permitted duplicate files in database.             
                # Get current time to save created_on time in database.
                created_on = timezone.now()
                # get current size of a file.
                initial_size  = os.path.getsize(file_path)
                log(f"Initial metadata saved for {file_path}. Monitoring file size...", level=logging.DEBUG)
               # Create the SourceData object and save it in the database.
                source_data = SourceData.objects.create(
                    SourceDatasetPath=file_path,
                    SourceDatasetName=image_name,
                    CreatedOn=created_on,
                    SourceDatasetSize=initial_size,
                    SourceDatasetType=source_data_type,
                    IsZippedFile=False,
                     IADSIngestionStatus="NA",
                    IADSCoreStatus="NA",
                )
                source_data.save()
                log(
                    "Source Data Created Successfully in Database.",
                    level=logging.DEBUG,
                    source_data=source_data,
                )                
                # Generate Excel File for the source file.
                log(
                    "Generating Excel/CSV File Paths for Source File.",
                    level=logging.DEBUG,
                )
                # create empty list to store excel/csv file paths.
                excel_file_paths = []
                # logic for creating Valid Excel/CSV files paths.
                for ext in valid_excel_extension:
                    excel_file_path = (
                        os.path.splitext(file_path)[0]
                        + "_"
                        + image_extension
                        + "."
                        + ext
                    )
                    excel_file_paths.append(excel_file_path)
                log(f"Excel file paths : {excel_file_paths}", level=logging.DEBUG)
                log(
                    "Checking if Excel/CSV File exists and if it exists then update IsExcelPresent Flag True.",
                    level=logging.DEBUG,
                    source_data=source_data,
                )
                found = False
                # Checking with each file path in list of excel files paths.
                for path in excel_file_paths:
                    # Checking if excel file exists.
                    if os.path.exists(path):
                        # set flag to true.
                        found = True
                        break
                # if Excel File exists then update IsExcelPresent flag if true.
                if found:
                    source_data.IsExcelPresent = True
                    source_data.save()
                    log(
                        "Excel/CSV File is Present hence Updated IsExcelPresent as True.",
                        level=logging.INFO,
                        source_data=source_data,
                    )
            elif image_extension in valid_excel_extension:
                # Created Source File Name for Excel File.
                new_source_data_name = self.generate_source_dataset_name(image_name)

                # check if source file name is generated or not.
                if new_source_data_name:
                    log(
                        f"Source File Name for Perticular Excel File and Name is : {new_source_data_name}.",
                        level=logging.DEBUG,
                    )

                    try:
                        # Getting Source Data object from databse related to file path in source_data_path list.,
                        log(
                            "Checking if Source Data File Exists in Database or not.",
                            level=logging.DEBUG,
                        )
                        source_data = SourceData.objects.get(
                            SourceDatasetName=new_source_data_name
                        )

                        log("Source Data File Exists in Database.", level=logging.DEBUG)
                        log(
                            f"For this Excel File Source File Exists in Hot Folder."
                            f" and Source File Path is : {source_data.SourceDatasetPath}.",
                            level=logging.DEBUG,
                        )

                        # If file path present in database then set IsExcelPresent flag True.
                        log(
                            "Set IsExcelPresent flag True for Perticular Source File in database.",
                            level=logging.DEBUG,
                        )
                        source_data.IsExcelPresent = True
                        source_data.save()

                    except Exception as e:
                        log(
                            f"For this Excel file Source File Does not Exists: {str(e)} and {traceback.format_exc()}",
                            level=logging.WARNING,
                        )
                else:
                    log(
                        "Source File Name Can't Generated Becuase Excel File Format is not Valid.",
                        level=logging.WARNING,
                    )
            else:
                log(
                    "Invalid File Extension. Expected tiff or excel File Extension.",
                    level=logging.ERROR,
                )
    
    def on_modified(self, event):
        log(
            "Reached at the 'on_modified' Event & About to Execute.", level=logging.DEBUG
        )
        if event.is_directory:
            return
        # Obtaining File Path of a File within HotFolder.
        # log("Obtaining File Path of a File within HotFolder.", level=logging.DEBUG)
        file_path = event.src_path
        # Extract File Name Only from Whole File Path.
        # log("Extract File Name Only from Whole File Path.", level=logging.DEBUG)
        image_name = os.path.basename(file_path)
        # Getting Extension of a File.
        # log("Getting Extension of a File.", level=logging.DEBUG)
        image_extension = image_name.split(".")[-1]
        # current_directory_name = os.path.basename(os.path.dirname(file_path))
        # Check if the file extension is valid.
        if image_extension in valid_extensions:
            try:
                source_data = SourceData.objects.get(SourceDatasetPath=file_path)
                # Getting previous size of a file from database.
                if source_data.IADSCoreStatus == WorkerName + "_" + Completed:
                    log(f"File {file_path} is already stabilized. Skipping further processing.", level=logging.INFO)
                    return
                previous_size = source_data.SourceDatasetSize
                    # Getting current size of a file.
                new_size = os.path.getsize(file_path)
                    # Now, proceed with the logic to monitor file size stabilization.
                log(f"Monitoring file size for stabilization: {file_path}", level=logging.DEBUG)                
                    # Polling mechanism to check if file size has stabilized.
                size_stable = False
                last_size = new_size
                log(f"Last recorded size of the file: {last_size}", level=logging.DEBUG)
                while not size_stable:
                        time.sleep(10)  # Wait for 10 seconds before checking again
                        current_size = os.path.getsize(file_path)
                        if current_size == last_size:
                            size_stable = True
                        else:
                            last_size = current_size
                log(f"File size stabilized for {image_name}. Final size: {current_size}.", level=logging.DEBUG)               
                    # Update the database entry with the final stabilized size.
                if current_size == last_size:
                        # Update the database entry with the final stabilized size.
                        source_data.SourceDatasetSize = current_size
                        source_data.save()
                        log(
                            f"Updated SourceData with stabilized file size for {file_path}.",
                            level=logging.DEBUG,
                            source_data=source_data,
                        )
                        difference = new_size - previous_size
                        # Checking if difference is greater than zero or not.
                        if difference <= 0:
                            log("File Uploading Finished.", level=logging.INFO, source_data=source_data)
                            source_data.SourceDatasetSize = new_size
                            source_data.ModifiedOn = timezone.now()
                            source_data.IADSIngestionStatus = ReadyForIngestion
                            source_data.IADSCoreStatus = WorkerName + "_" + Completed
                            source_data.save()
                            log("Source Data Updated Successfully in Database.", level=logging.DEBUG, source_data=source_data)
                            log("Well Done!!! File Watcher completed successfully!!",level=logging.DEBUG, source_data=source_data)
                        else:
                            log("The File Uploading in Progress.", level=logging.INFO, source_data=source_data)
                    
            except SourceData.DoesNotExist:
                    log("Oops!! File Does Not Exist in SourceData Table.", file=image_name, level=logging.ERROR)

            except SourceData.MultipleObjectsReturned:
                    log("Oops!! Multiple Source Data Files Returned from SourceData Table.", file=image_name, level=logging.WARNING)


class FileWatcherWorker(BaseWorker):
    """FileWatcherWorker is monitoring files in HotFolder."""

    def __init__(self) -> None:
        super().__init__()

    def process_started_worker(self):
        log("FileWatcherWorker Started.", level=logging.INFO)

        log("Getting HotFolder Path.", level=logging.INFO)

        # Getting HotFolder Path.
        hot_folder_path = settings.HOT_FOLDER

        log(
            f"Obtained HotFolder Path & HotFolderPath is : {hot_folder_path}.",
            level=logging.INFO,
        )

        log("Starting Event Handler.", level=logging.DEBUG)

        # Instantiate the HotFolderEventHandler class to handle file system events.
        event_handler = HotFolderEventHandler()

        # Calling the add_untracked_file_in_database method to add untracked files to the database.
        log(
            "Calling 'add_untracked_file_in_database' Method.",
            level=logging.DEBUG,
        )

        # calling add_untracked_file_in_database method to add missing files to the database.
       # event_handler.add_untracked_file_in_database()

        # log("Added Untracked File in database Successfully.", level=logging.DEBUG)

        log("Starting Observer.", level=logging.DEBUG)

        # Create an instance of the Observer class to observe file system events.
       # observer = Observer()
        observer = PollingObserver()

        log(f"Schedule Observer for HotFolder: {hot_folder_path}.", level=logging.DEBUG)

        # Schedule the observer to watch the specified path with recursive monitoring.
        observer.schedule(event_handler, path=hot_folder_path, recursive=True)

        log(
            f"Observer Scheduled Successfully for {hot_folder_path}.",
            level=logging.DEBUG,
        )

        try:
            # Start the observer to begin monitoring.
            observer.start()
            log("Monitoring for File Changes.", level=logging.DEBUG)
            log(
                f"Monitoring FileWatcher for This HotFolder Path : {hot_folder_path}.",
                level=logging.DEBUG,
            )
            while True:
                # Filter all source data objects from SourceData table in which IADSIngestionStatus is "NA"
                source_data_files = SourceData.objects.filter(IADSIngestionStatus="NA")

                # Iterate Through Each Source Data file in Filtered Source Data files.
                for source_data in source_data_files:


                    # Check if the file has already stabilized
                    if source_data.IADSCoreStatus == WorkerName + "_" + Completed:
                        log(f"File {source_data.SourceDatasetPath} is already stabilized. Skipping further processing.", level=logging.INFO)
                        continue  # Skip to the next file

                    file_path = source_data.SourceDatasetPath
                    previous_size = source_data.SourceDatasetSize


                    # Get the current size of the file
                    new_size = os.path.getsize(file_path)
                    log(f"Monitoring file size for stabilization: {file_path}", level=logging.DEBUG)

                    # Polling mechanism to check if file size has stabilized
                    size_stable = False
                    last_size = new_size
                    log(f"Last recorded size of the file: {last_size}", level=logging.DEBUG)

                    while not size_stable:
                        time.sleep(10)  # Wait for 10 seconds before checking again
                        current_size = os.path.getsize(file_path)

                        if current_size == last_size:
                            size_stable = True
                        else:
                            last_size = current_size

                    log(f"File size stabilized for {file_path}. Final size: {current_size}.", level=logging.DEBUG)

                     # Update the database entry with the final stabilized size
                    if current_size == last_size:
                        source_data.SourceDatasetSize = current_size
                        source_data.save()
                        log(f"Updated SourceData with stabilized file size for {file_path}.", level=logging.DEBUG, source_data=source_data)

                        difference = new_size - previous_size

                        # Check if the difference is greater than zero or not
                        if difference <= 0:
                            log("File Uploading Finished.", level=logging.INFO, source_data=source_data)
                            source_data.SourceDatasetSize = new_size
                            source_data.ModifiedOn = timezone.now()
                            source_data.IADSIngestionStatus = ReadyForIngestion
                            source_data.IADSCoreStatus = WorkerName + "_" + Completed
                            source_data.save()
                            log("This is inprogress file.", level=logging.DEBUG, source_data=source_data)
                            log("Source Data Updated Successfully in Database.", level=logging.DEBUG, source_data=source_data)
                            log("Well Done!!! File Watcher completed successfully!!", level=logging.DEBUG, source_data=source_data)
                        else:
                            log("The File Uploading in Progress.", level=logging.INFO, source_data=source_data)
                # After size stabilization is completed, call add_untracked_file_in_database.
                log("Calling 'add_untracked_file_in_database' Method.", level=logging.DEBUG)
                event_handler.add_untracked_file_in_database()
                time.sleep(20)

        except KeyboardInterrupt:
            observer.stop()
            observer.join()
        log("Monitoring Stopped.", level=logging.INFO)

    def start(self):
        self.process_started_worker()
