# Generated by Django 2.2.5 on 2019-10-01 18:40

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('seqr', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('gene_lists', '0002_genelist_seqr_locus_list'),
        ('base', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='analysedby',
            name='family',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='analysedby',
            name='seqr_family_analysed_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.FamilyAnalysedBy'),
        ),
        migrations.AddField(
            model_name='analysedby',
            name='user',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='analysisstatus',
            name='family',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='analysisstatus',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='causalvariant',
            name='family',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='cohort',
            name='individuals',
            field=models.ManyToManyField(to='base.Individual'),
        ),
        migrations.AddField(
            model_name='cohort',
            name='project',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='family',
            name='analysis_status_saved_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='family',
            name='project',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='family',
            name='seqr_family',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.Family'),
        ),
        migrations.AddField(
            model_name='familygroup',
            name='families',
            field=models.ManyToManyField(to='base.Family'),
        ),
        migrations.AddField(
            model_name='familygroup',
            name='project',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='familygroup',
            name='seqr_analysis_group',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.AnalysisGroup'),
        ),
        migrations.AddField(
            model_name='familyimageslide',
            name='family',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='familysearchflag',
            name='family',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='familysearchflag',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='genenote',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='individual',
            name='case_review_status_last_modified_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='individual',
            name='cram_file_path',
            field=models.TextField(blank=True, default=''),
        ),
        migrations.AddField(
            model_name='individual',
            name='family',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='individual',
            name='project',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='individual',
            name='seqr_individual',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.Individual'),
        ),
        migrations.AddField(
            model_name='individual',
            name='vcf_files',
            field=models.ManyToManyField(blank=True, to='base.VCFFile'),
        ),
        migrations.AddField(
            model_name='individualphenotype',
            name='individual',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Individual'),
        ),
        migrations.AddField(
            model_name='individualphenotype',
            name='phenotype',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.ProjectPhenotype'),
        ),
        migrations.AddField(
            model_name='project',
            name='collaborators',
            field=models.ManyToManyField(blank=True, through='base.ProjectCollaborator', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='project',
            name='created_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='project',
            name='gene_lists',
            field=models.ManyToManyField(null=True, through='base.ProjectGeneList', to='gene_lists.GeneList'),
        ),
        migrations.AddField(
            model_name='project',
            name='private_reference_populations',
            field=models.ManyToManyField(blank=True, to='base.ReferencePopulation'),
        ),
        migrations.AddField(
            model_name='project',
            name='seqr_project',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.Project'),
        ),
        migrations.AddField(
            model_name='projectcollaborator',
            name='project',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='projectcollaborator',
            name='user',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='projectgenelist',
            name='gene_list',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='gene_lists.GeneList'),
        ),
        migrations.AddField(
            model_name='projectgenelist',
            name='project',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='projectphenotype',
            name='project',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='projecttag',
            name='project',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='projecttag',
            name='seqr_variant_tag_type',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.VariantTagType'),
        ),
        migrations.AddField(
            model_name='userprofile',
            name='user',
            field=models.OneToOneField(null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='variantfunctionaldata',
            name='family',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='variantfunctionaldata',
            name='seqr_variant_functional_data',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.VariantFunctionalData'),
        ),
        migrations.AddField(
            model_name='variantfunctionaldata',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='variantnote',
            name='family',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Family'),
        ),
        migrations.AddField(
            model_name='variantnote',
            name='individual',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Individual'),
        ),
        migrations.AddField(
            model_name='variantnote',
            name='project',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project'),
        ),
        migrations.AddField(
            model_name='variantnote',
            name='seqr_variant_note',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.VariantNote'),
        ),
        migrations.AddField(
            model_name='variantnote',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='varianttag',
            name='seqr_variant_tag',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='seqr.VariantTag'),
        ),
        migrations.AddField(
            model_name='varianttag',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL),
        ),
    ]