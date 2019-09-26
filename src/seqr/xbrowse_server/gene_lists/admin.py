from django.contrib import admin
from xbrowse_server.gene_lists.models import GeneList


@admin.register(GeneList)
class GeneListAdmin(admin.ModelAdmin):
    fields = ['slug', 'name', 'description']
    search_fields = ['slug', 'name', 'description']
    save_on_top = True
    list_per_page = 2000

