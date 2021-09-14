# %% [markdown]
# # Generate datasets for competition

# %%

# %%

# /user/piccardi/ImageCaptioningCompetition/
base_output_path = 'images/datasets'
resnet_embeddings_training = f'{base_output_path}/training/resnet_embeddings/'
image_pixels_training = f'{base_output_path}/training/image_pixels/'
resnet_embeddings_test_val = f'{base_output_path}/test_val/resnet_embeddings/'
image_pixels_test_val = f'{base_output_path}/test_val/image_pixels/'

# %%

# Load the images URLs that passed all the filters:
valid_images = (spark
    .read
    .parquet("/user/piccardi/ImageCaptioningCompetition/all_valid_images.parquet")
    .select("image_url")
    .distinct())

valid_images

# %% [markdown]
# - Load the WIT training set from JSON
# - Remove the lines without caption (caption_reference_description)
# - Join with the images to filter.

# %%
training_wit = (spark.read.json("/user/piccardi/ImageCaptioningCompetition/wit_v1.train.all-0000*-of-00010.json.gz")
    .selectExpr('language', 'page_url', 'image_url', 'page_title', 
        'section_title', 'hierarchical_section_title', 
        'caption_reference_description', 'caption_attribution_description', 
        'caption_alt_text_description', 'mime_type', 'original_height', 
        'original_width', 'is_main_image', 'attribution_passes_lang_id', 
        'page_changed_recently', 'context_page_description', 
        'context_section_description')
    .where("caption_reference_description IS NOT NULL AND LENGTH(caption_reference_description)>0")\
    .distinct()
    .join(valid_images, "image_url"))
    


# %%
# Get the unique images of the training set:
uniqiue_images_training = training_wit.select("image_url").distinct()


# %% [markdown]
# ------------
# RESNET embedding  
# The rows are filtered to keep only the element of the training set that passed all the filters.

# %%
resnet_train = (spark
    .sql("SELECT * FROM aikochou.embeddings")
    .select("image_file_name", "features", "image_url")
    .join(unique_images_training, "image_url")
    .selectExpr("image_url", "features as embedding")
    .cache())

@F.udf(returnType='string')
def serialize_embedding(embeddings):
    embeddings_tsv = ",".join([str(e) for e in embeddings])
    return embeddings_tsv    

doit = False
if doit:
    (resnet_train
        .select('image_url', serialize_embedding('embedding'))
        .repartition(215)
        .write
        .csv(resnet_embeddings_training,mode='overwrite',compression='gzip',sep='\t')
    )

#%%

pixel_training = (spark
    .read.format("avro")
    .load("/user/fab/images/competition/train/pixels/")
    .join(unique_images_training, "image_url")
    .selectExpr("image_url", "image.image_bytes_b64 as image_bytes_b64")
    .cache())

# %% [markdown]
# ------------
# Raw image pixels
# The rows are filtered to keep only the element of the training set that passed all the filters.

# %%
# Add the field _metadata_url_ containing the page on commons with the copyright information
@F.udf(returnType='string')
def generate_url(image_url):
    file_name = image_url.split("/")[-1]
    return "http://commons.wikimedia.org/wiki/File:"+file_name


# %%
pixel_training = pixel_training.withColumn("metadata_url", generate_url('image_url'))

doit = False
if doit:
    (pixel_training        
        .repartition(200) 
        .write
        .csv(image_pixels_training,mode='overwrite',compression='gzip',sep='\t')
    )


# %% [markdown]
# ------------
# Reading datasets for testing


@F.udf(returnType='array<float>')
def parse_embedding(emb_str):
    return [float(e) for e in emb_str.split(',')]

# parse embedding array
first_emb = (spark.read
    .csv(path=resnet_embeddings_training+'*.csv.gz',sep="\t")
    .select(F.col('_c0').alias('image_url'), parse_embedding('_c1').alias('embedding'))
    .take(1)[0]
)

print(len(first_emb.embedding))
# 2048

# %%

first_image = (spark
    .read.csv(path=image_pixels_training+'*.csv.gz',sep="\t")
    .select(F.col('_c0').alias('image_url'), F.col('_c1').alias('b64_bytes'),F.col('_c2').alias('metadata_url'))
    .take(1)[0]
)

# parse image bytes
import base64
from io import BytesIO
from PIL import Image
pil_image = Image.open(BytesIO(base64.b64decode(first_image.b64_bytes)))
print(pil_image.size)
# (300, 159)

# %% [markdown]
# ------------
# Testing & Validation datasets


# %%
# fair_test_val = spark.read.json("images/wit_val_test_selected_rows_50K.jsonl")
fair_test_val = spark.read.json("images/wit_val_test_selected_rows_100K.jsonl")
fair_test_val.select('row.page_url').count()


# %%

test_wit = (spark.read.json("/user/piccardi/ImageCaptioningCompetition/wit_v1.test.all-0000*-of-00005.json.gz")
    .selectExpr('language', 'page_url', 'image_url', 'page_title', 
        'section_title', 'hierarchical_section_title', 
        'caption_reference_description', 'caption_attribution_description', 
        'caption_alt_text_description', 'mime_type', 'original_height', 
        'original_width', 'is_main_image', 'attribution_passes_lang_id', 
        'page_changed_recently', 'context_page_description', 
        'context_section_description')
    .where("caption_reference_description IS NOT NULL AND LENGTH(caption_reference_description)>0")\
    .distinct()
    .join(valid_images, "image_url")
    .cache())
 
val_wit = (spark.read.json("/user/piccardi/ImageCaptioningCompetition/wit_v1.val.all-0000*-of-00005.tsv.gz")
    .selectExpr('language', 'page_url', 'image_url', 'page_title', 
        'section_title', 'hierarchical_section_title', 
        'caption_reference_description', 'caption_attribution_description', 
        'caption_alt_text_description', 'mime_type', 'original_height', 
        'original_width', 'is_main_image', 'attribution_passes_lang_id', 
        'page_changed_recently', 'context_page_description', 
        'context_section_description')
    .where("caption_reference_description IS NOT NULL AND LENGTH(caption_reference_description)>0")\
    .distinct()
    .join(valid_images, "image_url")
    .cache())

#%% 

test_val_wit = test_wit.union(val_wit).cache()

#%%
unique_images_validation_test = (test_val_wit
    .join(fair_test_val.select('row.page_url'), 'page_url')
    .select("image_url")
    .distinct())

#%%
print(unique_images_validation_test.count())
# 22893

# %%

#%%

# the pixel data for the test&val datasets are not unique per image, as the input
# dataset was accidentally the (article,image) caption pair and images can be used
# by multiple articles. for that reason, .distinct() is used to make a unique collection
# of images

pixel_test_val = (spark
    .read.format("avro")
    .load(["/user/fab/images/competition/test/pixels/", "/user/fab/images/competition/validation/pixels/"])
    .select(
        F.col("image_url"), 
        F.col("image.image_bytes_b64").alias("image_bytes_b64"),
        generate_url("image_url").alias("metadata_url"))
    .distinct()
    .join(unique_images_validation_test, "image_url")
)
# %%
# print(pixel_test_val.count())
# unique_images_validation_test

doit = False
if doit:
    (pixel_test_val        
        .repartition(5) 
        .write
        .csv(image_pixels_test_val,mode='overwrite',compression='gzip',sep='\t') 
    )   


# %%

# the resnet embeddings for the test&val datasets are not unique per image, as the input
# dataset was accidentally the (article,image) caption pair and images can be used
# by multiple articles. for that reason, .distinct() is used to make a unique collection
# of images

resnet_test = (spark
    .sql("SELECT * FROM aikochou.embeddings_test")
    .select("image_file_name", "features", "image_url")
    .groupBy('image_url').agg(F.first('features').alias('features'))
    .join(unique_images_validation_test, "image_url")
    .selectExpr("image_url", "features as embedding"))
resnet_val = (spark
    .sql("SELECT * FROM aikochou.embeddings_val")
    .select("image_file_name", "features", "image_url")
    .groupBy('image_url').agg(F.first('features').alias('features'))
    .join(unique_images_validation_test, "image_url")
    .selectExpr("image_url", "features as embedding"))

resnet_test_val_merged = resnet_test.union(resnet_val).cache()


# %%

# %%
print(resnet_test_val_merged.count())
# features

#%%
doit = False
if doit:
    (resnet_test_val_merged   
        .select('image_url', serialize_embedding('embedding'))     
        .repartition(10) 
        .write
        .csv(resnet_embeddings_test_val,mode='overwrite',compression='gzip',sep='\t') 
    )   


# %%
print(spark
    .read.csv(path=image_pixels_test_val+'*.csv.gz',sep="\t")
    .select(F.col('_c0').alias('image_url'), F.col('_c1').alias('b64_bytes'),F.col('_c2').alias('metadata_url'))
    .count()
)
# 44762

#%%
print(spark.read
    .csv(path=resnet_embeddings_test_val+'*.csv.gz',sep="\t")
    .select(F.col('_c0').alias('image_url'), parse_embedding('_c1').alias('embedding'))
    .count()
)
# 44762
# %%
