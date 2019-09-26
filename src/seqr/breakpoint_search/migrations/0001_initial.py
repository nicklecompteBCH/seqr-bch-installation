# Generated by Django 2.2 on 2019-09-09 21:19

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('base', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Breakpoint',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('xpos', models.BigIntegerField(db_index=True)),
                ('obs', models.IntegerField(db_index=True)),
                ('sample_count', models.IntegerField(db_index=True)),
                ('consensus', models.FloatField()),
                ('partner', models.TextField(blank=True, null=True)),
                ('individual', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='base.Individual')),
                ('project', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='base.Project')),
            ],
            options={
                'db_table': 'base_breakpoint',
            },
        ),
        migrations.CreateModel(
            name='BreakpointMetaData',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('type', models.TextField(blank=True, default='')),
                ('tags', models.TextField(blank=True, default='')),
                ('breakpoint', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='breakpoint_search.Breakpoint')),
            ],
            options={
                'db_table': 'base_breakpointmetadata',
            },
        ),
        migrations.CreateModel(
            name='BreakpointGene',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('gene_symbol', models.CharField(db_index=True, max_length=20)),
                ('cds_dist', models.IntegerField()),
                ('breakpoint', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='breakpoint_search.Breakpoint')),
            ],
            options={
                'db_table': 'base_breakpointgene',
            },
        ),
        migrations.CreateModel(
            name='BreakpointFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file_path', models.CharField(blank=True, default='', max_length=500)),
                ('project', models.ForeignKey(blank=True, on_delete=django.db.models.deletion.PROTECT, to='base.Project')),
            ],
            options={
                'db_table': 'base_breakpointfile',
            },
        ),
    ]
