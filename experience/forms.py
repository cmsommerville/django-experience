from django import forms
from django.core.validators import FileExtensionValidator

class DateInput(forms.DateInput):
    input_type = 'date'

class ExperienceFormManual(forms.Form):
    fromdate = forms.DateField(
        required = False,
        widget=DateInput(
            format = ["%Y-%m-%d"],
            attrs = {'placeholder': 'From Date', 'class': 'form-control'}
            )
        )
    thrudate = forms.DateField(
        required = False,
        widget=DateInput(
            format = ["%Y-%m-%d"],
            attrs = {'placeholder': 'Thru Date', 'class': 'form-control'}
            )
        )
    grpnum = forms.CharField(required = False, widget=forms.TextInput(attrs = {'placeholder': 'Group Number', 'class': 'form-control'}))
    title = forms.CharField(required = False, widget=forms.TextInput(attrs = {'placeholder': 'Report Title', 'class': 'form-control'}))
    file = forms.FileField(
        required = False,
        widget=forms.FileInput(attrs = {'style': 'display:none;'}),
        validators = [FileExtensionValidator(allowed_extensions=["json"])])
