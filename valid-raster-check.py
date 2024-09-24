from osgeo import gdal

dataset = gdal.Open('your_file.tif')

if dataset is not None:
    print("Raster is valid.")
    print("Metadata:", dataset.GetMetadata())
else:
    print("Raster is invalid or could not be opened.")

