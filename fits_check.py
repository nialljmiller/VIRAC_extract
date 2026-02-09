from primvs_api import PrimvsCatalog

cat = PrimvsCatalog("/media/bigdata/PRIMVS/light_curves/")
results = cat.get_lightcurves_from_fits("reclass.fits", id_column="sourceid")

# results is {source_id: DataFrame, ...}
for sid, lc in results.items():
    print(sid, len(lc), "detections")
