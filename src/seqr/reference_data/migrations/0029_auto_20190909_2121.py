# Generated by Django 2.2 on 2019-09-09 21:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reference_data', '0028_auto_20190909_2119'),
    ]

    operations = [
        migrations.AlterField(
            model_name='geneexpression',
            name='tissue_type',
            field=models.CharField(choices=[('muscle', 'muscle'), ('kidney', 'kidney'), ('small_intestine', 'small_intestine'), ('liver', 'liver'), ('uterus', 'uterus'), ('breast', 'breast'), ('skin', 'skin'), ('adrenal_gland', 'adrenal_gland'), ('salivary_gland', 'salivary_gland'), ('testis', 'testis'), ('nerve', 'nerve'), ('esophagus', 'esophagus'), ('pituitary', 'pituitary'), ('stomach', 'stomach'), ('cells_-_transformed_fibroblasts', 'cells_-_transformed_fibroblasts'), ('prostate', 'prostate'), ('thyroid', 'thyroid'), ('whole_blood', 'whole_blood'), ('colon', 'colon'), ('pancreas', 'pancreas'), ('spleen', 'spleen'), ('lung', 'lung'), ('vagina', 'vagina'), ('brain', 'brain'), ('cells_-_ebv-transformed_lymphocytes', 'cells_-_ebv-transformed_lymphocytes'), ('heart', 'heart'), ('blood_vessel', 'blood_vessel'), ('adipose_tissue', 'adipose_tissue'), ('ovary', 'ovary')], max_length=40),
        ),
    ]
