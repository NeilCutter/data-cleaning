from flask import Flask, render_template, request, flash, redirect, url_for
import pandas
import glob
import warnings
import os

app = Flask(__name__)
app.secret_key = "keep it secret, keep it safe"

from webapp import routes
