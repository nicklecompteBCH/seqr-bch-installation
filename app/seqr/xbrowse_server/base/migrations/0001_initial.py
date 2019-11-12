# Generated by Django 2.2 on 2019-09-09 21:19

from django.db import migrations, models
import django.db.models.deletion
from typing import List


class Migration(migrations.Migration):

    initial = True

    dependencies : List[str] = [
    ]

    operations = [
        migrations.CreateModel(
            name='AnalysedBy',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_saved', models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name='AnalysisStatus',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_saved', models.DateTimeField(null=True)),
                ('status', models.CharField(choices=[('S', 'Solved'), ('S_kgfp', 'Solved - known gene for phenotype'), ('S_kgdp', 'Solved - gene linked to different phenotype'), ('S_ng', 'Solved - novel gene'), ('Sc_kgfp', 'Strong candidate - known gene for phenotype'), ('Sc_kgdp', 'Strong candidate - gene linked to different phenotype'), ('Sc_ng', 'Strong candidate - novel gene'), ('Rcpc', 'Reviewed, currently pursuing candidates'), ('Rncc', 'Reviewed, no clear candidate'), ('I', 'Analysis in Progress'), ('Q', 'Waiting for data')], default='I', max_length=10)),
            ],
        ),
        migrations.CreateModel(
            name='CausalVariant',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('variant_type', models.CharField(default='', max_length=10)),
                ('xpos', models.BigIntegerField(null=True)),
                ('ref', models.TextField(null=True)),
                ('alt', models.TextField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Cohort',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('cohort_id', models.CharField(blank=True, default='', max_length=140)),
                ('display_name', models.CharField(blank=True, default='', max_length=140)),
                ('short_description', models.CharField(blank=True, default='', max_length=140)),
                ('variant_stats_json', models.TextField(blank=True, default='')),
            ],
        ),
        migrations.CreateModel(
            name='Family',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('family_id', models.CharField(blank=True, default='', max_length=140)),
                ('family_name', models.CharField(blank=True, default='', max_length=140)),
                ('short_description', models.TextField(blank=True, default='')),
                ('about_family_content', models.TextField(blank=True, default='')),
                ('analysis_summary_content', models.TextField(blank=True, default='')),
                ('pedigree_image', models.ImageField(blank=True, null=True, upload_to='pedigree_images')),
                ('pedigree_image_height', models.IntegerField(blank=True, default=0, null=True)),
                ('pedigree_image_width', models.IntegerField(blank=True, default=0, null=True)),
                ('analysis_status', models.CharField(choices=[('S', 'S'), ('S_kgfp', 'S'), ('S_kgdp', 'S'), ('S_ng', 'S'), ('Sc_kgfp', 'S'), ('Sc_kgdp', 'S'), ('Sc_ng', 'S'), ('Rcpc', 'R'), ('Rncc', 'R'), ('I', 'A'), ('Q', 'W')], default='Q', max_length=10)),
                ('analysis_status_date_saved', models.DateTimeField(blank=True, null=True)),
                ('internal_analysis_status', models.CharField(blank=True, choices=[('S', 'S'), ('S_kgfp', 'S'), ('S_kgdp', 'S'), ('S_ng', 'S'), ('Sc_kgfp', 'S'), ('Sc_kgdp', 'S'), ('Sc_ng', 'S'), ('Rcpc', 'R'), ('Rncc', 'R'), ('I', 'A'), ('Q', 'W')], max_length=10, null=True)),
                ('causal_inheritance_mode', models.CharField(default='unknown', max_length=20)),
                ('internal_case_review_notes', models.TextField(blank=True, default='', null=True)),
                ('internal_case_review_summary', models.TextField(blank=True, default='', null=True)),
                ('coded_phenotype', models.TextField(blank=True, default='', null=True)),
                ('post_discovery_omim_number', models.TextField(blank=True, default='', null=True)),
                ('combined_families_info', models.TextField(blank=True, default='')),
            ],
        ),
        migrations.CreateModel(
            name='FamilyGroup',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('slug', models.SlugField(max_length=100)),
                ('name', models.CharField(max_length=100)),
                ('description', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='FamilyImageSlide',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('image', models.ImageField(blank=True, null=True, upload_to='family_image_slides')),
                ('order', models.FloatField(default=0.0)),
                ('caption', models.CharField(blank=True, default='', max_length=300)),
            ],
        ),
        migrations.CreateModel(
            name='FamilySearchFlag',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('xpos', models.BigIntegerField()),
                ('ref', models.TextField()),
                ('alt', models.TextField()),
                ('flag_type', models.CharField(choices=[('C', 'Likely causal'), ('R', 'Flag for review'), ('N', 'Other note')], max_length=1)),
                ('suggested_inheritance', models.SlugField(default='', max_length=40)),
                ('search_spec_json', models.TextField(blank=True, default='')),
                ('date_saved', models.DateTimeField()),
                ('note', models.TextField(blank=True, default='')),
            ],
        ),
        migrations.CreateModel(
            name='GeneNote',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('note', models.TextField(blank=True, default='')),
                ('gene_id', models.CharField(max_length=20)),
                ('date_saved', models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name='Individual',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_date', models.DateTimeField(auto_now_add=True, null=True)),
                ('indiv_id', models.SlugField(blank=True, default='', max_length=140)),
                ('maternal_id', models.SlugField(blank=True, default='', max_length=140, null=True)),
                ('paternal_id', models.SlugField(blank=True, default='', max_length=140, null=True)),
                ('gender', models.CharField(choices=[('M', 'Male'), ('F', 'Female'), ('U', 'Unknown')], default='U', max_length=1)),
                ('affected', models.CharField(choices=[('A', 'Affected'), ('N', 'Unaffected'), ('U', 'Unknown')], default='U', max_length=1)),
                ('nickname', models.CharField(blank=True, default='', max_length=140)),
                ('other_notes', models.TextField(blank=True, default='', null=True)),
                ('case_review_status', models.CharField(choices=[('N', 'Not In Review'), ('I', 'In Review'), ('U', 'Uncertain'), ('A', 'Accepted'), ('R', 'Not Accepted'), ('Q', 'More Info Needed'), ('P', 'Pending Results and Records'), ('W', 'Waitlist'), ('WD', 'Withdrew'), ('IE', 'Ineligible'), ('DP', 'Declined to Participate')], default='I', max_length=2)),
                ('case_review_status_last_modified_date', models.DateTimeField(blank=True, db_index=True, null=True)),
                ('case_review_discussion', models.TextField(blank=True, null=True)),
                ('phenotips_patient_id', models.CharField(blank=True, db_index=True, max_length=30, null=True)),
                ('phenotips_id', models.SlugField(blank=True, default='', max_length=165)),
                ('phenotips_data', models.TextField(blank=True, default='', null=True)),
                ('mean_target_coverage', models.FloatField(blank=True, null=True)),
                ('coverage_status', models.CharField(choices=[('S', 'In Sequencing'), ('I', 'Interim'), ('C', 'Complete'), ('A', 'Abandoned')], default='S', max_length=1)),
                ('vcf_id', models.CharField(blank=True, default='', max_length=40)),
                ('in_case_review', models.BooleanField(default=False)),
                ('bam_file_path', models.TextField(blank=True, default='')),
                ('coverage_file', models.TextField(blank=True, default='')),
                ('cnv_bed_file', models.TextField(blank=True, default='')),
                ('exome_depth_file', models.TextField(blank=True, default='')),
                ('combined_individuals_info', models.TextField(blank=True, default='')),
            ],
        ),
        migrations.CreateModel(
            name='IndividualPhenotype',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('boolean_val', models.NullBooleanField()),
                ('float_val', models.FloatField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Project',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('genome_version', models.CharField(default='37', max_length=3)),
                ('project_id', models.SlugField(blank=True, default='', max_length=500, unique=True)),
                ('project_name', models.TextField(blank=True, default='')),
                ('description', models.TextField(blank=True, default='')),
                ('created_date', models.DateTimeField(blank=True, null=True)),
                ('last_accessed_date', models.DateTimeField(blank=True, null=True)),
                ('is_phenotips_enabled', models.BooleanField(default=False)),
                ('phenotips_user_id', models.TextField(blank=True, db_index=True, null=True)),
                ('is_mme_enabled', models.BooleanField(default=True)),
                ('mme_primary_data_owner', models.TextField(blank=True, default='Samantha Baxter', null=True)),
                ('mme_contact_url', models.TextField(blank=True, default='mailto:matchmaker@broadinstitute.org', null=True)),
                ('mme_contact_institution', models.TextField(blank=True, default='Broad Center for Mendelian Genomics', null=True)),
                ('is_functional_data_enabled', models.BooleanField(default=False)),
                ('disease_area', models.CharField(blank=True, choices=[('blood', 'Blood'), ('cardio', 'Cardio'), ('kidney', 'Kidney'), ('muscle', 'Muscle'), ('neurodev', 'Neurodev'), ('orphan_disease', 'Orphan Disease'), ('retinal', 'Retinal')], max_length=20, null=True)),
                ('default_control_cohort', models.CharField(blank=True, default='', max_length=100)),
                ('disable_staff_access', models.BooleanField(default=False)),
                ('is_public', models.BooleanField(default=False)),
                ('combined_projects_info', models.TextField(blank=True, default='')),
            ],
        ),
        migrations.CreateModel(
            name='ProjectCollaborator',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('collaborator_type', models.CharField(choices=[('manager', 'Manager'), ('collaborator', 'Collaborator')], default='collaborator', max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='ProjectGeneList',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='ProjectPhenotype',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('slug', models.SlugField(default='pheno', max_length=140)),
                ('name', models.CharField(default='', max_length=140)),
                ('category', models.CharField(choices=[('disease', 'Disease'), ('clinial_observation', 'Clinical Observation'), ('other', 'Other')], max_length=20)),
                ('datatype', models.CharField(choices=[('bool', 'Boolean'), ('number', 'Number')], max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='ProjectTag',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('tag', models.TextField()),
                ('category', models.TextField(default='')),
                ('title', models.TextField(default='')),
                ('color', models.CharField(default='', max_length=10)),
                ('order', models.FloatField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='ReferencePopulation',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('slug', models.SlugField(default='')),
                ('name', models.CharField(default='', max_length=100)),
                ('file_type', models.CharField(default='', max_length=50)),
                ('file_path', models.CharField(default='', max_length=500)),
                ('is_public', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='UserProfile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('display_name', models.CharField(blank=True, default='', max_length=100)),
                ('set_password_token', models.CharField(blank=True, default='', max_length=40)),
            ],
        ),
        migrations.CreateModel(
            name='VariantFunctionalData',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('functional_data_tag', models.TextField()),
                ('metadata', models.TextField(null=True)),
                ('date_saved', models.DateTimeField(null=True)),
                ('xpos', models.BigIntegerField()),
                ('ref', models.TextField()),
                ('alt', models.TextField()),
                ('search_url', models.TextField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='VariantNote',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('note', models.TextField(blank=True, default='')),
                ('submit_to_clinvar', models.BooleanField(default=False)),
                ('xpos', models.BigIntegerField()),
                ('ref', models.TextField()),
                ('alt', models.TextField()),
                ('date_saved', models.DateTimeField()),
                ('search_url', models.TextField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='VCFFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file_path', models.TextField(blank=True, default='')),
                ('dataset_type', models.CharField(choices=[('VARIANTS', 'Variant Calls'), ('SV', 'SV Calls')], default='VARIANTS', max_length=10)),
                ('sample_type', models.CharField(blank=True, choices=[('WES', 'Exome'), ('WGS', 'Whole Genome'), ('RNA', 'RNA'), ('ARRAY', 'ARRAY')], max_length=3, null=True)),
                ('elasticsearch_index', models.TextField(blank=True, db_index=True, null=True)),
                ('loaded_date', models.DateTimeField(blank=True, null=True)),
                ('project', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='base.Project')),
            ],
        ),
        migrations.CreateModel(
            name='VariantTag',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_saved', models.DateTimeField(null=True)),
                ('xpos', models.BigIntegerField()),
                ('ref', models.TextField()),
                ('alt', models.TextField()),
                ('search_url', models.TextField(null=True)),
                ('family', models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family')),
                ('project_tag', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='base.ProjectTag')),
            ],
        ),
    ]
