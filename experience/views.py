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
                request.session["REPORT_LOG"] = batch.batchLog

        #return render(request, "app/runlog.html", {"log": request.session["REPORT_LOG"]})
        return HttpResponseRedirect("/runlog/")

    return render(request, "app/index.html", {'form': form})



# Create your views here.
def runlog(request):
    context = {"log": request.session["REPORT_LOG"]}
    return render(request, "app/runlog.html", context = context)

# Create your views here.
def reports(request, report_name):
    return render(request, report_name + ".html")
