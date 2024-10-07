# Importing necessary libraries and modules.
import datetime
import os
import shutil
import traceback
from ._base_logger import Logger
from ._base_worker import BaseWorker
from IngestionEngine.models import SourceData, TargetData, TargetImage
import logging
from osgeo import gdal
from django.conf import settings
import time
# Creating a logger instance.
log = Logger("CogGeneratorWorker").get_logger()

# Constants defining various worker statuses and paths.
PrevoiusWorkerStatus = "IngesterWorker_Completed"
WorkerName = "CogGeneratorWorker"
InProgress = "InProgress"
Completed = "Completed"
Failed = "Failed"
Scheduled = "Scheduled"
Pause = "Pause"

pair_files = {".img": [".ige", ".tif.aux.xml", ".rrd", ".rde"], ".tif": [".tfw"]}
pair_tiff_paths = []

class CogGeneratorWorker(BaseWorker):
    """Cog Generator Worker is generate cog of item at destination path."""
    def __init__(self) -> None:
        super().__init__()
    # Method to find items that are ready for CogGenerator Worker.
    def find_eligible_items(self):
        """Find items that are ready for Cog Generator Worker."""
        try:
            log("Searching eligible items for CogGeneratorWorker.", level=logging.DEBUG)
            source_datas = SourceData.objects.filter(
                IADSCoreStatus__in=(
                    "LocalIngesterWorker_Completed",
                    f"{WorkerName}_{Failed}",
                )
            )
            if len(source_datas) == 0:
                log(f"No SourceData found for {WorkerName}", level=logging.WARN)
            return source_datas
        except Exception as e:
            log(
                f"Oops!! Error has Occured while searching eligible items and error is: {str(e)} and {traceback.format_exc()}",
                level=logging.ERROR,
            )
            raise e

    # Method to check eligibility of an item for CogGenerator Worker.
    def eligible_item_reader(self, source_data: SourceData):
        """Find eligibility of item to start Cog Generator Worker."""
        try:
            log(
                "Reached at the “eligible_item_reader” Method & About to Execute.",
                source_data=source_data,
                level=logging.DEBUG,
            )
            # get the previous worker status from the database.
            previous_worker_status = source_data.IADSCoreStatus
            # Check if status from database is "IngesterWorker_Completed"
            if previous_worker_status in (
                "LocalIngesterWorker_Completed",
                f"{WorkerName}_{Failed}",
            ):
                log(
                    "Well Done!! Item is Eligible to execute CogGeneratorWorker.",
                    source_data=source_data,
                    level=logging.INFO,
                )
                return True
            else:
                log(
                    "Oops!!.Not an Eligible Item & item InProgress or might be Already Processed.",
                    source_data=source_data,
                    level=logging.WARNING,
                )
                return False
        except Exception as e:
            log(
                f"Oops!! Error has Occured while checking Eligibility of PreIngestionWorker and error is: {str(e)} and {traceback.format_exc()}",
                source_data=source_data,
                level=logging.ERROR,
            )
            raise e

    # Method to convert TIFF to COG.
    def convert_tiff_to_cog(
        self,
        input_tiff_path,
        local_cog_path,
        source_dataset_path,
        source_data: SourceData,
        compression="DEFLATE",
    ):
        log(
            "Reached at the “convert_tiff_to_cog” Method & About to Execute.",
            source_data=source_data,
            level=logging.DEBUG,
        )
        try:
            # Register the GDAL drivers.
            gdal.AllRegister()
            log(
                f"Opening tiff file using GDAL. {input_tiff_path}",
                source_data=source_data,
                level=logging.DEBUG,
            )
            # Open the input TIFF file using GDAL.
            input_ds = gdal.Open(input_tiff_path)
            if input_ds is None:
                log(
                    "Oops!! Error has Occured while Reading the Input TIFF file.",
                    source_data=source_data,
                    level=logging.ERROR,
                )
                return False
            else:
                log(
                    "Great!! File Opened Successfully through GDAL.",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                log(
                    "Checking description.",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                description = input_ds.GetDescription()
                log(
                    f"Description from GDAL: {description}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
            # Add a delay of 60 minutes (3600 seconds) to simulate long processing time
        #     log(
        #         "Adding a delay of 10 minutes before generating COG to test for errors.",
        #         source_data=source_data,
        #         level=logging.DEBUG,
        #     )
        #     time.sleep(600)  # Sleep for 60 minutes
        #     log(
        #     "Delay completed. Proceeding with COG generation.",
        #     source_data=source_data,
        #     level=logging.DEBUG,
        # )
            # Check file size
            log(
                "Checking file size.",
                source_data=source_data,
                level=logging.DEBUG,
            )
            ext = os.path.splitext(source_dataset_path)[1]
            log(
                f"Extension of Source File Path is: {ext}",
                source_data=source_data,
                level=logging.DEBUG,
            )
            if ext in pair_files:
                base_name = os.path.splitext(source_dataset_path)[0]
                log(
                    f"Base name is: {base_name}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                filename = os.path.basename(base_name)
                log(
                    f"Filename is: {filename}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                for new_ext in pair_files[ext]:
                    if new_ext == ".tif.aux.xml":
                        continue
                    elif settings.GENERATE_COG_ON_LOCAL == "YES":
                        pair_file_path = os.path.join(
                            settings.LOCAL_INGESTED_FOLDER, filename + new_ext
                        )
                        pair_tiff_paths.append(pair_file_path)
                    else:
                        pair_file_path = os.path.join(
                            os.path.dirname(input_tiff_path), filename + new_ext
                        )
                        log(
                            f"Pair File path is : {pair_file_path}",
                            source_data=source_data,
                            level=logging.DEBUG,
                        )
                        pair_tiff_paths.append(pair_file_path)
                log(
                    f"Pair tiff paths are: {pair_tiff_paths}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                # Initialize the pair file size
                pair_file_size = 0

                # Calculate the total size of all files
                for file_path in pair_tiff_paths:
                    if os.path.isfile(file_path):  # Check if the file exists
                        file_size = os.path.getsize(file_path)  # Get the file size
                        pair_file_size += file_size
                file_size = pair_file_size + os.path.getsize(input_tiff_path)
                log(
                    f"Total size which contains main source file and its associated pair files: {file_size}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
            else:
                file_size = os.path.getsize(input_tiff_path)
                log(
                    f"file size is: {file_size}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
            if (
                file_size > 4 * 1024 * 1024 * 1024
            ):  # Check if file size is greater than 4GB
                # Use BigTIFF format if file size is greater than 4GB
                log(
                    "File size is greater than 4GB hence converting to bigtiff format.",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                output_format = "COG"
                output_format_options = ["BIGTIFF=YES", "NUM_THREADS=ALL_CPUS"]
            else:
                log(
                    "File size is less than 4GB.",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
                output_format = "COG"
                output_format_options = ["BIGTIFF=YES","NUM_THREADS=ALL_CPUS"]
            # Create a new COG file.
            log(
                f"About to Generate COG of a File and  COG Path is: {local_cog_path}",
                source_data=source_data,
                level=logging.INFO,
            )
            log( "--------------Database connection close successfully again-----------.", level=logging.INFO,)
            connection.close()
            try:
                # Generate the COG using GDAL Translate.
                output_ds = gdal.Translate(
                    local_cog_path,
                    input_ds,
                    format=output_format,
                    creationOptions=output_format_options,
                )
            except Exception as e:
                log(
                    f"Oops!! Error has Occured while Generating the COG and error is: {str(e)} and {traceback.format_exc()}",
                    source_data=source_data,
                    level=logging.ERROR,
                )
                raise e
            connection.connect()
            log( "--------------Database connection connect successfully again-----------.", level=logging.INFO,)
            log(
                    f"output_ds: {output_ds}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )
            log(
                    f"input_ds: {input_ds}",
                    source_data=source_data,
                    level=logging.DEBUG,
                )           
            # Check if the output dataset is None, indicating an error during COG generation.
            if output_ds is None:
                log(
                    "Oops!! Error has Occured while generating the COG.",
                    source_data=source_data,
                    level=logging.ERROR,
                )
                return False
            # Close datasets
            input_ds = None
            output_ds = None
            return True
        except Exception as e:
            log(
                f"Oops!! Unfortunatly, Found an Error again while Generating COG file and error is: {str(e)} and {traceback.format_exc()}",
                source_data=source_data,
                level=logging.ERROR,
            )
            raise e

    def process_started_worker(self, source_data: SourceData):
        log(
            "Reached at the 'process_started_worker' Method & About to Execute.",
            source_data=source_data,
            level=logging.DEBUG,
        )
        log(
            "Well Done!! CogGeneratorWorker Started.",
            source_data=source_data,
            level=logging.INFO,
        )
        # Update the worker status in the database.
        IADSCoreStatus = WorkerName + "_" + InProgress
        source_data.IADSCoreStatus = IADSCoreStatus
        source_data.save()
        log(
            "Getting File Path of a File from Source Data Table in Database.",
            source_data=source_data,
            level=logging.DEBUG,
        )
        log(
            "Great!! File Path obtained Successfully from Database.",
            source_data=source_data,
            level=logging.INFO,
        )
        log(
            "Getting Target Data object Related to Source Data form Database.",
            source_data=source_data,
            level=logging.DEBUG,
        )
        # Getting TargetData object related to SourceData form Database.
        target_data = TargetData.objects.get(SourceData=source_data)
        source_dataset_path = source_data.SourceDatasetPath
        log(
            "Successfully Got Target Data object from Database.",
            source_data=source_data,
            target_data=target_data,
            level=logging.DEBUG,
        )
        image: TargetImage
        target_images = target_data.TargetImages.all()
        log(
            f"Need to Create COG for {len(target_images)} Images.",
            source_data=source_data,
            target_data=target_data,
            level=logging.DEBUG,
        )
        for i, image in enumerate(target_images):
            # Filter out already done
            if not image.IsCOGGenerated:
                file_name, ext = os.path.splitext(os.path.basename(image.LocalTiffPath))
                log(
                    f"[{i}] Generating COG Path for file and path is: {image.LocalTiffPath}",
                    level=logging.DEBUG,
                    source_data=source_data,
                    target_data=target_data,
                )
                image.LocalCOGPath = (
                    f"{os.path.splitext(image.LocalTiffPath)[0]}_cog.tif"
                )
                image.save()
                log(
                    "If GENERATE_COG_ON_LOCAL is True then Generate COG on local machine.",
                    source_data=source_data,
                    target_data=target_data,
                    level=logging.DEBUG,
                )
                if settings.GENERATE_COG_ON_LOCAL == "YES":
                    log(
                        "Generating COG on Local Machine.",
                        source_data=source_data,
                        target_data=target_data,
                        level=logging.DEBUG,
                    )
                    local_tiff_path = image.LocalTiffPath
                    local_cog_path = image.LocalCOGPath

                    nas_tiff_path = local_tiff_path.replace(
                        settings.LOCAL_INGESTED_FOLDER, settings.SERVER_PATH
                    )
                    # Define the correct base path
                    base_path = settings.LOCAL_INGESTED_FOLDER

                    log( f"base_path when GENERATE_COG_ON_LOCAL is yes: {base_path}",
                        source_data=source_data,
                        target_data=target_data,
                        level=logging.DEBUG,)
                    
                    if settings.CREAT_MONTH_FOLDER_ON_INGESTED_RESOURCE == "YES":
                        # Get the current month and year
                        now = datetime.datetime.now()
                        month_year = now.strftime("%b_%y")  # Example: "Jun_24"
                        log(f"Formatted month_year: {month_year}",
                            source_data=source_data,
                            target_data=target_data,
                            level=logging.DEBUG,)
                        # Create the new directory path
                        new_directory_path = os.path.join(base_path, month_year)
                        log(
                            f"Path with month and year is: {new_directory_path}",
                            source_data=source_data,
                            target_data=target_data,
                            level=logging.DEBUG,
                        )
                        # Ensure the directory is created if it doesn't exist
                        if not os.path.exists(new_directory_path):
                            os.makedirs(new_directory_path, exist_ok=True)
                            log(
                                f"Directory created: {new_directory_path}",
                                source_data=source_data,
                                level=logging.INFO,
                            )
                        else:
                            log(
                                f"Directory already exists: {new_directory_path}",
                                source_data=source_data,
                                level=logging.DEBUG,
                            )
                    else:
                        # If month folder creation is disabled, use the base path directly
                        new_directory_path = base_path
                        log(
                            f"New directory path without month folder: {new_directory_path}",
                            source_data=source_data,
                            target_data=target_data,
                            level=logging.DEBUG,
                        )
                    # Check if the new directory exists, if not, create it
                    if not os.path.exists(new_directory_path):
                        os.makedirs(new_directory_path)
                        log(
                            f"Directory created: {new_directory_path}",
                            source_data=source_data,
                            target_data=target_data,
                            level=logging.DEBUG,
                        )
                    else:
                        log(
                            f"Directory already exists: {new_directory_path}",
                            source_data=source_data,
                            target_data=target_data,
                            level=logging.DEBUG,
                        )
                    new_nas_tiff_path = nas_tiff_path.replace(
                        base_path, new_directory_path
                    )
                    # log(
                    #     f"NAS Drive tiff path is: {new_nas_tiff_path}",
                    #     source_data=source_data,
                    #     target_data=target_data,
                    #     level=logging.DEBUG, )

                    nas_tiff_dir = os.path.dirname(new_nas_tiff_path)
                    # log(
                    #     f"NAS tiff Directory is:{nas_tiff_dir}",
                    #     source_data=source_data,
                    #     target_data=target_data,
                    #     level=logging.DEBUG,
                    # )
                    if not os.path.exists(nas_tiff_dir):
                        log(
                            f"Making directory at: {nas_tiff_dir}",
                            source_data=source_data,
                            target_data=target_data,
                            level=logging.DEBUG,
                        )
                        os.makedirs(nas_tiff_dir, exist_ok=True)

                    image.Path = new_nas_tiff_path
                    image.save()

                    nas_cog_path = local_cog_path.replace(
                        settings.LOCAL_INGESTED_FOLDER,
                        settings.SERVER_PATH,
                    )
                    new_nas_cog_path = nas_cog_path.replace(
                        base_path, new_directory_path
                    )
                    image.COGPath = new_nas_cog_path
                    image.save()

                    image.IsCOGGenerated = self.convert_tiff_to_cog(
                        local_tiff_path,
                        local_cog_path,
                        source_dataset_path,
                        source_data,
                    )
                    image.save()
                else:
                    log(
                        "Generating COG on NAS Drive.",
                        source_data=source_data,
                        target_data=target_data,
                        level=logging.DEBUG,
                    )
                    image.Path = image.TargetData.TargetDatasetPath
                    log(
                        f"NAS Drive Tiff path is: {image.Path} .",
                        source_data=source_data,
                        target_data=target_data,
                        level=logging.DEBUG,
                    )
                    image.COGPath = image.LocalCOGPath.replace(
                        os.path.dirname(image.LocalCOGPath),
                        os.path.dirname(image.TargetData.TargetDatasetPath),
                    )
                    log(
                        f"NAS Drive COG path is: {image.COGPath} .",
                        source_data=source_data,
                        target_data=target_data,
                        level=logging.DEBUG,
                    )
                    image.save()
                    image.IsCOGGenerated = self.convert_tiff_to_cog(
                        image.Path,
                        image.COGPath,
                        source_dataset_path,
                        source_data,
                    )
                    image.save()

                image.DownloadURL = settings.SERVER_URL + image.Path.replace(
                    settings.SERVER_PATH, ""
                )
                log(
                    f"DownloadURL Generated Successfully and URL is:{image.DownloadURL}",
                    level=logging.INFO,
                    source_data=source_data,
                    target_data=target_data,
                )

                image.VisualizationURL = settings.SERVER_URL + image.COGPath.replace(
                    settings.SERVER_PATH, ""
                )
                log(
                    f"VisualizationURL Generated Successfully and URL is:{image.VisualizationURL}",
                    level=logging.INFO,
                    source_data=source_data,
                    target_data=target_data,
                )
                image.save()

        log(
            "Checking if the COG of a File is Created Successfully or not.",
            source_data=source_data,
            target_data=target_data,
            level=logging.INFO,
        )
        # Check if for every image, cog is generated
        if not target_data.TargetImages.filter(IsCOGGenerated=False).exists():
            source_data.IADSCoreStatus = WorkerName + "_" + Completed
            source_data.save()
            target_data.BusinessMetadata.IsEditable = False
            target_data.BusinessMetadata.save()
            source_data.save()
            log(
                "Well Done!! COG of a File is Created Successfully.",
                source_data=source_data,
                target_data=target_data,
                level=logging.INFO,
            )

            log(
                "Well Done!! CogGeneratorWorker Completed Successfully.",
                source_data=source_data,
                target_data=target_data,
                level=logging.INFO,
            )
        else:
            # Update the worker status in the database.
            IADSCoreStatus = WorkerName + "_" + Failed
            source_data.IADSCoreStatus = IADSCoreStatus
            source_data.IADSIngestionStatus = "Ingestion_Failed"
            source_data.target_data.IADSIngestionStatus = "Ingestion_Failed"
            source_data.target_data.save()
            source_data.save()
            log(
                "CogGeneratorWorker Failed & COG might not be Created.",
                source_data=source_data,
                target_data=target_data,
                level=logging.ERROR,
            )
            raise ValueError("Error.COG might not be Created.")

    def start(self, source_data: SourceData):
        log(
            "Checking if the Item is Eligible to Start CogGeneratorWorker.",
            source_data=source_data,
            level=logging.DEBUG,
        )
        # Calling eligible_item_reader to find is item is eligible to start or not.
        is_eligible = self.eligible_item_reader(source_data)
        if is_eligible:
            log(
                "Now starting CogGeneratorWorker.",
                source_data=source_data,
                level=logging.DEBUG,
            )
            # Calling process_started_worker method.
            self.process_started_worker(source_data)






        """
        log(
            "Filter First Record from SDReder Table which is having Active Status.",
            level=logging.INFO,
        )
        nas_drive = SDReader.objects.filter(Status="Active").first()
        log(
            "Took First Active Drive from SDReader Table",
            level=logging.DEBUG,
            nas_drive_path=nas_drive.FolderPath,
        )

        if nas_drive is None:
            log("There are No Active Drives Present.", level=logging.ERROR)
            return None  # Return None if no active drives are found.

        folder_path = nas_drive.FolderPath
        folder_info = self.folder_space_info(folder_path)
        log(f"Folder information:{folder_info}", level=logging.DEBUG)
        log(
            "Updating nas_drive columns in SDReader Table.",
            source_data=source_data,
            target_data=target_data,
            level=logging.DEBUG,
        )
        nas_drive.TotalCapacity = folder_info["TotalCapacity"]
        nas_drive.FreeCapacity = folder_info["FreeCapacity"]
        nas_drive.UsedCapacity = folder_info["UsedCapacity"]
        nas_drive.save()
        log(
            "nas_drive storage information updated successfully in SDReader Table.",
            source_data=source_data,
            target_data=target_data,
        )
        """
