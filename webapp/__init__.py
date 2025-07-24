from flask import Flask, render_template, request, flash, redirect, url_for
import pandas
import glob
import warnings
import os
import datetime
import numpy as np
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import types

app = Flask(__name__)
app.secret_key = "keep it secret, keep it safe"

from webapp import routes
