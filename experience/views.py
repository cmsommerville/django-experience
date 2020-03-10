from django.shortcuts import render
from . import forms
import json
from . import data_handling

# Create your views here.
def index(request):
    form = forms.ExperienceFormManual()

    if request.method == "POST":
        form = forms.ExperienceFormManual(request.POST, request.FILES)

        if form.is_valid():
            if request.FILES['file'] is None:
                print("From Date: " + str(form.cleaned_data['fromdate']))
                print("Thru Date: " + str(form.cleaned_data['thrudate']))
                print("Group Number: " + form.cleaned_data['grpnum'])
                print("Report Title: " + form.cleaned_data['title'])
                print("File: " + request.FILES['file'].name)
            else:
                data = json.loads(request.FILES['file'].read())
                experience = data_handling.ExperienceReport(**data)
                experience.reportLoop()
                print(experience.request_data)

    return render(request, "app/index.html", {'form': form})
