from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader
from . import forms
import json
from . import data_handling
from experience.models.batch import Batch

# Create your views here.
def index(request):
    form = forms.ExperienceFormManual()

    if request.method == "POST":
        form = forms.ExperienceFormManual(request.POST, request.FILES)

        if form.is_valid():
            if request.FILES['file'] is None:
                pass
            else:
                data = json.loads(request.FILES['file'].read())
                batch = Batch(**data)
                batch.run()

        return render(request, "app/runlog.html", {"log": batch.batchLog})
        #return HttpResponseRedirect("/runlog/")

    return render(request, "app/index.html", {'form': form})



# Create your views here.
def runlog(request):
    return render(request, "app/runlog.html")

# Create your views here.
def reports(request, report_name):
    return render(request, report_name + ".html")
