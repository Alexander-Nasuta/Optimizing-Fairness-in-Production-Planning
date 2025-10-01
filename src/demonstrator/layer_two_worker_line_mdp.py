import copy
import pprint
import sys
from collections import namedtuple
from datetime import datetime
import pytz

import gymnasium as gym
import numpy as np
import pandas as pd
import numpy.typing as npt

from utils.crf_timestamp_solver_time_conversion import timestamp_to_solver_time, solver_time_to_timestamp
from utils.logger import log
from typing import Any, SupportsFloat, List, Tuple, Hashable

start_timestamp = 1759125600  # corresponds to Monday 2025-09-29 06:00:00

cp_layer_one_output = [
{'Finish': 1759269600,
  'Order': 'Order 1',
  'Resource': 'Line 3',
  'Start': 1759197600,
  'Task': 'Order 1 × Geometry 1',
  'geometry': 'Geometry 1'},
 {'Finish': 1759154400,
  'Order': 'Order 2',
  'Resource': 'Line 3',
  'Start': 1759125600,
  'Task': 'Order 2 × Geometry 2',
  'geometry': 'Geometry 2'},
 {'Finish': 1759197600,
  'Order': 'Order 3',
  'Resource': 'Line 3',
  'Start': 1759154400,
  'Task': 'Order 3 × Geometry 3',
  'geometry': 'Geometry 3'},
 {'Finish': 1759183200,
  'Order': 'Order 4',
  'Resource': 'Line 2',
  'Start': 1759125600,
  'Task': 'Order 4 × Geometry 4',
  'geometry': 'Geometry 4'},
 {'Finish': 1759240800,
  'Order': 'Order 5',
  'Resource': 'Line 2',
  'Start': 1759183200,
  'Task': 'Order 5 × Geometry 5',
  'geometry': 'Geometry 5'},
 {'Finish': 1759269600,
  'Order': 'Order 1',
  'Resource': 'Line 2',
  'Start': 1759240800,
  'Task': 'Order 1 × Geometry 6',
  'geometry': 'Geometry 6'},
 {'Finish': 1759269600,
  'Order': 'Order 1',
  'Resource': 'Line 1',
  'Start': 1759212000,
  'Task': 'Order 1 × Geometry 7',
  'geometry': 'Geometry 7'},
 {'Finish': 1759212000,
  'Order': 'Order 2',
  'Resource': 'Line 1',
  'Start': 1759125600,
  'Task': 'Order 2 × Geometry 8',
  'geometry': 'Geometry 8'}
]

worker_availabilities = [
{'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_1'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_1'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_2'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_2'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_3'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_3'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_4'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_4'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_5'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_5'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_6'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_6'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_7'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_7'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_8'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_8'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_9'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_9'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_10'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_10'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_11'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_11'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_12'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_12'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_13'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_13'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_14'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_14'},
 {'date': '2025-09-29',
  'end_timestamp': 1759154400,
  'from_timestamp': 1759125600,
  'worker': 'w_15'},
 {'date': '2025-09-30',
  'end_timestamp': 1759240800,
  'from_timestamp': 1759212000,
  'worker': 'w_15'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_16'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_16'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_17'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_17'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_18'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_18'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_19'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_19'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_20'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_20'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_21'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_21'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_22'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_22'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_23'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_23'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_24'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_24'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_25'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_25'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_26'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_26'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_27'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_27'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_28'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_28'},
 {'date': '2025-09-29',
  'end_timestamp': 1759183200,
  'from_timestamp': 1759154400,
  'worker': 'w_29'},
 {'date': '2025-09-30',
  'end_timestamp': 1759269600,
  'from_timestamp': 1759240800,
  'worker': 'w_29'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_30'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_31'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_32'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_33'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_34'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_35'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_36'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_37'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_38'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_39'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_40'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_41'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_42'},
 {'date': '2025-09-29',
  'end_timestamp': 1759212000,
  'from_timestamp': 1759183200,
  'worker': 'w_43'}
]

geometry_line_mapping = [
    {
        "geometry": "Geometry 1",
        "main_line": "Line 3",
        "alternative_lines": [],
        "number_of_workers": 3,
    },
    {
        "geometry": "Geometry 2",
        "main_line": "Line 3",
        "alternative_lines": [],
        "number_of_workers": 4,
    },
    {
        "geometry": "Geometry 3",
        "main_line": "Line 3",
        "alternative_lines": [],
        "number_of_workers": 3,
    },
    {
        "geometry": "Geometry 4",
        "main_line": "Line 2",
        "alternative_lines": [],
        "number_of_workers": 6,
    },
    {
        "geometry": "Geometry 5",
        "main_line": "Line 2",
        "alternative_lines": [],
        "number_of_workers": 5,
    },
    {
        "geometry": "Geometry 6",
        "main_line": "Line 2",
        "alternative_lines": [],
        "number_of_workers": 6,
    },
    {
        "geometry": "Geometry 7",
        "main_line": "Line 1",
        "alternative_lines": [],
        "number_of_workers": 5,
    },
    {
        "geometry": "Geometry 8",
        "main_line": "Line 1",
        "alternative_lines": [],
        "number_of_workers": 6,
    },
]

human_factor_data = [
{'experience': 0.74,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.96,
  'resilience': 0.61,
  'worker': 'w_1'},
 {'experience': 0.97,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.63,
  'resilience': 0.24,
  'worker': 'w_1'},
 {'experience': 0.64,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.24,
  'resilience': 0.71,
  'worker': 'w_1'},
 {'experience': 0.06,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.14,
  'resilience': 0.76,
  'worker': 'w_1'},
 {'experience': 0.95,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.99,
  'resilience': 0.16,
  'worker': 'w_1'},
 {'experience': 0.08,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.49,
  'resilience': 0.23,
  'worker': 'w_1'},
 {'experience': 0.62,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.51,
  'resilience': 0.55,
  'worker': 'w_1'},
 {'experience': 0.97,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.71,
  'resilience': 0.93,
  'worker': 'w_1'},
 {'experience': 0.38,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.36,
  'resilience': 0.79,
  'worker': 'w_2'},
 {'experience': 0.04,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.45,
  'resilience': 0.7,
  'worker': 'w_2'},
 {'experience': 0.17,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.94,
  'resilience': 0.09,
  'worker': 'w_2'},
 {'experience': 0.98,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.49,
  'resilience': 0.7,
  'worker': 'w_2'},
 {'experience': 0.91,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.62,
  'resilience': 0.36,
  'worker': 'w_2'},
 {'experience': 0.22,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.29,
  'resilience': 0.69,
  'worker': 'w_2'},
 {'experience': 0.79,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.86,
  'resilience': 0.16,
  'worker': 'w_2'},
 {'experience': 0.8,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.33,
  'resilience': 0.72,
  'worker': 'w_2'},
 {'experience': 0.08,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.82,
  'resilience': 0.4,
  'worker': 'w_3'},
 {'experience': 0.62,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.98,
  'worker': 'w_3'},
 {'experience': 0.95,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.19,
  'worker': 'w_3'},
 {'experience': 0.45,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.11,
  'resilience': 0.64,
  'worker': 'w_3'},
 {'experience': 0.87,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.33,
  'resilience': 0.09,
  'worker': 'w_3'},
 {'experience': 0.88,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.29,
  'resilience': 0.19,
  'worker': 'w_3'},
 {'experience': 0.28,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.89,
  'resilience': 0.14,
  'worker': 'w_3'},
 {'experience': 0.76,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.38,
  'resilience': 0.24,
  'worker': 'w_3'},
 {'experience': 0.08,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.99,
  'resilience': 0.59,
  'worker': 'w_4'},
 {'experience': 0.35,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.88,
  'resilience': 0.23,
  'worker': 'w_4'},
 {'experience': 0.86,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.87,
  'resilience': 0.42,
  'worker': 'w_4'},
 {'experience': 0.88,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.91,
  'resilience': 0.79,
  'worker': 'w_4'},
 {'experience': 0.85,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.39,
  'resilience': 0.74,
  'worker': 'w_4'},
 {'experience': 0.33,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.49,
  'resilience': 0.87,
  'worker': 'w_4'},
 {'experience': 0.51,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.13,
  'resilience': 0.08,
  'worker': 'w_4'},
 {'experience': 0.6,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.26,
  'resilience': 0.26,
  'worker': 'w_4'},
 {'experience': 0.51,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.65,
  'worker': 'w_5'},
 {'experience': 0.31,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.43,
  'resilience': 0.84,
  'worker': 'w_5'},
 {'experience': 0.09,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.26,
  'worker': 'w_5'},
 {'experience': 0.73,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.9,
  'resilience': 0.15,
  'worker': 'w_5'},
 {'experience': 0.43,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.63,
  'resilience': 0.08,
  'worker': 'w_5'},
 {'experience': 0.04,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.6,
  'worker': 'w_5'},
 {'experience': 0.15,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.53,
  'resilience': 0.88,
  'worker': 'w_5'},
 {'experience': 0.7,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.19,
  'worker': 'w_5'},
 {'experience': 0.81,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.61,
  'resilience': 0.6,
  'worker': 'w_6'},
 {'experience': 0.76,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.69,
  'resilience': 0.17,
  'worker': 'w_6'},
 {'experience': 0.43,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.66,
  'resilience': 0.19,
  'worker': 'w_6'},
 {'experience': 0.03,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.48,
  'worker': 'w_6'},
 {'experience': 0.44,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.58,
  'resilience': 0.84,
  'worker': 'w_6'},
 {'experience': 0.43,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.61,
  'resilience': 0.19,
  'worker': 'w_6'},
 {'experience': 0.86,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.27,
  'resilience': 0.1,
  'worker': 'w_6'},
 {'experience': 0.92,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.65,
  'resilience': 0.84,
  'worker': 'w_6'},
 {'experience': 0.05,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.13,
  'worker': 'w_7'},
 {'experience': 0.36,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.95,
  'resilience': 0.62,
  'worker': 'w_7'},
 {'experience': 0.28,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.28,
  'resilience': 0.75,
  'worker': 'w_7'},
 {'experience': 1.0,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.44,
  'worker': 'w_7'},
 {'experience': 0.29,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.11,
  'resilience': 0.17,
  'worker': 'w_7'},
 {'experience': 0.21,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.91,
  'resilience': 0.54,
  'worker': 'w_7'},
 {'experience': 0.23,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.07,
  'resilience': 0.89,
  'worker': 'w_7'},
 {'experience': 0.89,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.86,
  'resilience': 0.89,
  'worker': 'w_7'},
 {'experience': 0.82,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.82,
  'resilience': 0.57,
  'worker': 'w_8'},
 {'experience': 0.51,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.85,
  'resilience': 0.69,
  'worker': 'w_8'},
 {'experience': 0.32,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.46,
  'worker': 'w_8'},
 {'experience': 0.85,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.97,
  'resilience': 0.85,
  'worker': 'w_8'},
 {'experience': 0.07,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.02,
  'resilience': 0.66,
  'worker': 'w_8'},
 {'experience': 0.51,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.4,
  'resilience': 0.05,
  'worker': 'w_8'},
 {'experience': 0.97,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.05,
  'resilience': 0.82,
  'worker': 'w_8'},
 {'experience': 0.57,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.34,
  'resilience': 0.17,
  'worker': 'w_8'},
 {'experience': 0.68,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.14,
  'worker': 'w_9'},
 {'experience': 0.59,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.89,
  'resilience': 0.49,
  'worker': 'w_9'},
 {'experience': 0.49,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.72,
  'resilience': 0.43,
  'worker': 'w_9'},
 {'experience': 0.49,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.09,
  'resilience': 0.08,
  'worker': 'w_9'},
 {'experience': 0.26,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.45,
  'resilience': 0.19,
  'worker': 'w_9'},
 {'experience': 0.35,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.65,
  'resilience': 0.1,
  'worker': 'w_9'},
 {'experience': 0.46,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.38,
  'resilience': 0.84,
  'worker': 'w_9'},
 {'experience': 0.67,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.04,
  'resilience': 0.53,
  'worker': 'w_9'},
 {'experience': 0.96,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.2,
  'resilience': 0.87,
  'worker': 'w_10'},
 {'experience': 0.48,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.82,
  'worker': 'w_10'},
 {'experience': 0.06,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.53,
  'worker': 'w_10'},
 {'experience': 0.69,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.49,
  'worker': 'w_10'},
 {'experience': 0.94,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.59,
  'resilience': 0.77,
  'worker': 'w_10'},
 {'experience': 0.77,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.37,
  'resilience': 0.1,
  'worker': 'w_10'},
 {'experience': 0.01,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.16,
  'resilience': 0.44,
  'worker': 'w_10'},
 {'experience': 0.56,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.26,
  'resilience': 0.11,
  'worker': 'w_10'},
 {'experience': 0.16,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.73,
  'resilience': 0.63,
  'worker': 'w_11'},
 {'experience': 0.3,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.48,
  'worker': 'w_11'},
 {'experience': 0.37,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.27,
  'resilience': 0.48,
  'worker': 'w_11'},
 {'experience': 0.2,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.96,
  'resilience': 0.83,
  'worker': 'w_11'},
 {'experience': 0.1,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.89,
  'resilience': 0.49,
  'worker': 'w_11'},
 {'experience': 0.76,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.39,
  'resilience': 0.24,
  'worker': 'w_11'},
 {'experience': 0.67,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.1,
  'resilience': 0.09,
  'worker': 'w_11'},
 {'experience': 0.24,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.81,
  'resilience': 0.42,
  'worker': 'w_11'},
 {'experience': 0.36,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.36,
  'resilience': 0.37,
  'worker': 'w_12'},
 {'experience': 0.23,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.58,
  'resilience': 0.01,
  'worker': 'w_12'},
 {'experience': 0.84,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.85,
  'resilience': 0.73,
  'worker': 'w_12'},
 {'experience': 0.37,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.98,
  'resilience': 0.26,
  'worker': 'w_12'},
 {'experience': 0.95,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 1.0,
  'resilience': 0.69,
  'worker': 'w_12'},
 {'experience': 0.21,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.56,
  'resilience': 0.19,
  'worker': 'w_12'},
 {'experience': 0.93,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.82,
  'resilience': 0.31,
  'worker': 'w_12'},
 {'experience': 0.37,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.95,
  'resilience': 0.83,
  'worker': 'w_12'},
 {'experience': 0.11,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.22,
  'resilience': 0.42,
  'worker': 'w_13'},
 {'experience': 0.6,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.06,
  'resilience': 0.55,
  'worker': 'w_13'},
 {'experience': 0.52,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.81,
  'worker': 'w_13'},
 {'experience': 0.71,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.87,
  'resilience': 0.13,
  'worker': 'w_13'},
 {'experience': 0.71,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.23,
  'resilience': 0.6,
  'worker': 'w_13'},
 {'experience': 0.11,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.38,
  'resilience': 0.08,
  'worker': 'w_13'},
 {'experience': 0.63,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.3,
  'resilience': 0.01,
  'worker': 'w_13'},
 {'experience': 0.4,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.36,
  'resilience': 0.15,
  'worker': 'w_13'},
 {'experience': 0.4,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.85,
  'resilience': 0.01,
  'worker': 'w_14'},
 {'experience': 0.75,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.53,
  'resilience': 0.99,
  'worker': 'w_14'},
 {'experience': 0.0,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.94,
  'resilience': 0.15,
  'worker': 'w_14'},
 {'experience': 0.7,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.18,
  'worker': 'w_14'},
 {'experience': 0.08,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.82,
  'resilience': 0.72,
  'worker': 'w_14'},
 {'experience': 0.09,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.8,
  'resilience': 0.42,
  'worker': 'w_14'},
 {'experience': 0.02,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.06,
  'resilience': 0.11,
  'worker': 'w_14'},
 {'experience': 0.18,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.38,
  'worker': 'w_14'},
 {'experience': 0.58,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.37,
  'resilience': 0.33,
  'worker': 'w_15'},
 {'experience': 0.65,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.69,
  'worker': 'w_15'},
 {'experience': 0.22,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.53,
  'resilience': 0.01,
  'worker': 'w_15'},
 {'experience': 0.22,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.38,
  'resilience': 0.15,
  'worker': 'w_15'},
 {'experience': 0.54,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.63,
  'resilience': 0.38,
  'worker': 'w_15'},
 {'experience': 0.57,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.73,
  'resilience': 0.55,
  'worker': 'w_15'},
 {'experience': 0.91,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.56,
  'worker': 'w_15'},
 {'experience': 0.22,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.37,
  'resilience': 0.61,
  'worker': 'w_15'},
 {'experience': 0.16,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.5,
  'resilience': 0.88,
  'worker': 'w_16'},
 {'experience': 0.84,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.81,
  'resilience': 0.01,
  'worker': 'w_16'},
 {'experience': 0.39,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.4,
  'resilience': 0.11,
  'worker': 'w_16'},
 {'experience': 0.37,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.59,
  'resilience': 0.53,
  'worker': 'w_16'},
 {'experience': 0.38,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.06,
  'worker': 'w_16'},
 {'experience': 0.56,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.12,
  'worker': 'w_16'},
 {'experience': 0.24,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.73,
  'worker': 'w_16'},
 {'experience': 0.21,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.23,
  'resilience': 0.14,
  'worker': 'w_16'},
 {'experience': 0.5,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.36,
  'resilience': 0.53,
  'worker': 'w_17'},
 {'experience': 0.96,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.79,
  'worker': 'w_17'},
 {'experience': 0.05,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.26,
  'resilience': 0.52,
  'worker': 'w_17'},
 {'experience': 0.71,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.22,
  'resilience': 0.05,
  'worker': 'w_17'},
 {'experience': 0.35,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.85,
  'resilience': 0.61,
  'worker': 'w_17'},
 {'experience': 0.54,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.88,
  'resilience': 0.4,
  'worker': 'w_17'},
 {'experience': 0.58,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.44,
  'resilience': 0.38,
  'worker': 'w_17'},
 {'experience': 0.68,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.1,
  'resilience': 0.56,
  'worker': 'w_17'},
 {'experience': 0.35,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.42,
  'resilience': 0.5,
  'worker': 'w_18'},
 {'experience': 0.75,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.17,
  'worker': 'w_18'},
 {'experience': 0.15,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.67,
  'resilience': 0.56,
  'worker': 'w_18'},
 {'experience': 0.39,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.96,
  'resilience': 0.4,
  'worker': 'w_18'},
 {'experience': 0.62,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.06,
  'resilience': 0.37,
  'worker': 'w_18'},
 {'experience': 0.1,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.19,
  'resilience': 0.64,
  'worker': 'w_18'},
 {'experience': 0.49,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.11,
  'resilience': 0.34,
  'worker': 'w_18'},
 {'experience': 0.49,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.87,
  'resilience': 0.21,
  'worker': 'w_18'},
 {'experience': 0.65,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.68,
  'resilience': 0.88,
  'worker': 'w_19'},
 {'experience': 0.2,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.21,
  'resilience': 0.26,
  'worker': 'w_19'},
 {'experience': 0.26,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.77,
  'resilience': 0.68,
  'worker': 'w_19'},
 {'experience': 0.38,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.83,
  'resilience': 0.88,
  'worker': 'w_19'},
 {'experience': 0.79,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.45,
  'resilience': 0.34,
  'worker': 'w_19'},
 {'experience': 0.17,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.93,
  'resilience': 0.18,
  'worker': 'w_19'},
 {'experience': 0.72,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.19,
  'resilience': 0.8,
  'worker': 'w_19'},
 {'experience': 0.21,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.97,
  'resilience': 0.12,
  'worker': 'w_19'},
 {'experience': 0.2,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.15,
  'resilience': 0.11,
  'worker': 'w_20'},
 {'experience': 0.71,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.69,
  'resilience': 0.25,
  'worker': 'w_20'},
 {'experience': 0.28,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.68,
  'resilience': 0.21,
  'worker': 'w_20'},
 {'experience': 0.78,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.99,
  'resilience': 0.96,
  'worker': 'w_20'},
 {'experience': 0.58,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.93,
  'resilience': 0.3,
  'worker': 'w_20'},
 {'experience': 0.0,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.6,
  'resilience': 0.8,
  'worker': 'w_20'},
 {'experience': 0.5,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.51,
  'resilience': 0.64,
  'worker': 'w_20'},
 {'experience': 0.19,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.1,
  'resilience': 0.29,
  'worker': 'w_20'},
 {'experience': 0.73,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.65,
  'resilience': 0.26,
  'worker': 'w_21'},
 {'experience': 0.03,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.31,
  'resilience': 0.69,
  'worker': 'w_21'},
 {'experience': 0.02,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.43,
  'resilience': 0.96,
  'worker': 'w_21'},
 {'experience': 0.53,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.26,
  'resilience': 0.6,
  'worker': 'w_21'},
 {'experience': 0.88,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.57,
  'resilience': 0.92,
  'worker': 'w_21'},
 {'experience': 0.52,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.36,
  'worker': 'w_21'},
 {'experience': 0.99,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.16,
  'resilience': 0.2,
  'worker': 'w_21'},
 {'experience': 0.48,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.05,
  'resilience': 0.56,
  'worker': 'w_21'},
 {'experience': 0.72,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.85,
  'resilience': 0.58,
  'worker': 'w_22'},
 {'experience': 0.77,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.71,
  'resilience': 0.12,
  'worker': 'w_22'},
 {'experience': 0.85,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.37,
  'resilience': 0.88,
  'worker': 'w_22'},
 {'experience': 0.79,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.41,
  'resilience': 0.44,
  'worker': 'w_22'},
 {'experience': 0.28,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.11,
  'resilience': 0.31,
  'worker': 'w_22'},
 {'experience': 0.14,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.28,
  'resilience': 0.9,
  'worker': 'w_22'},
 {'experience': 0.2,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.44,
  'resilience': 0.28,
  'worker': 'w_22'},
 {'experience': 0.69,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.64,
  'resilience': 0.42,
  'worker': 'w_22'},
 {'experience': 1.0,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.14,
  'resilience': 0.25,
  'worker': 'w_23'},
 {'experience': 0.72,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.41,
  'resilience': 0.51,
  'worker': 'w_23'},
 {'experience': 0.71,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.29,
  'resilience': 0.83,
  'worker': 'w_23'},
 {'experience': 0.39,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.37,
  'worker': 'w_23'},
 {'experience': 0.82,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.28,
  'resilience': 0.22,
  'worker': 'w_23'},
 {'experience': 0.3,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.56,
  'resilience': 0.46,
  'worker': 'w_23'},
 {'experience': 0.93,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.95,
  'resilience': 0.73,
  'worker': 'w_23'},
 {'experience': 0.22,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.58,
  'resilience': 0.04,
  'worker': 'w_23'},
 {'experience': 0.08,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.81,
  'resilience': 0.94,
  'worker': 'w_24'},
 {'experience': 0.24,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.78,
  'resilience': 0.69,
  'worker': 'w_24'},
 {'experience': 0.29,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.21,
  'worker': 'w_24'},
 {'experience': 0.42,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.68,
  'resilience': 0.25,
  'worker': 'w_24'},
 {'experience': 0.87,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.65,
  'resilience': 0.65,
  'worker': 'w_24'},
 {'experience': 0.01,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.5,
  'worker': 'w_24'},
 {'experience': 0.39,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.89,
  'resilience': 0.4,
  'worker': 'w_24'},
 {'experience': 0.72,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.83,
  'resilience': 0.43,
  'worker': 'w_24'},
 {'experience': 0.51,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.55,
  'resilience': 0.7,
  'worker': 'w_25'},
 {'experience': 0.58,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.56,
  'resilience': 0.58,
  'worker': 'w_25'},
 {'experience': 0.6,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.8,
  'resilience': 0.39,
  'worker': 'w_25'},
 {'experience': 0.02,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.72,
  'resilience': 0.91,
  'worker': 'w_25'},
 {'experience': 0.99,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.11,
  'resilience': 0.06,
  'worker': 'w_25'},
 {'experience': 0.6,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.43,
  'worker': 'w_25'},
 {'experience': 0.2,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.1,
  'resilience': 0.14,
  'worker': 'w_25'},
 {'experience': 0.35,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.97,
  'resilience': 0.46,
  'worker': 'w_25'},
 {'experience': 0.86,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.13,
  'resilience': 0.76,
  'worker': 'w_26'},
 {'experience': 0.52,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.27,
  'resilience': 0.67,
  'worker': 'w_26'},
 {'experience': 0.48,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.21,
  'resilience': 0.21,
  'worker': 'w_26'},
 {'experience': 0.93,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.54,
  'resilience': 0.44,
  'worker': 'w_26'},
 {'experience': 0.91,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.99,
  'resilience': 0.27,
  'worker': 'w_26'},
 {'experience': 0.88,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.5,
  'resilience': 0.82,
  'worker': 'w_26'},
 {'experience': 0.34,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.78,
  'resilience': 0.72,
  'worker': 'w_26'},
 {'experience': 0.92,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.93,
  'resilience': 0.79,
  'worker': 'w_26'},
 {'experience': 0.85,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.7,
  'resilience': 0.78,
  'worker': 'w_27'},
 {'experience': 0.04,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.27,
  'resilience': 0.2,
  'worker': 'w_27'},
 {'experience': 0.53,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.55,
  'resilience': 0.46,
  'worker': 'w_27'},
 {'experience': 0.66,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.94,
  'worker': 'w_27'},
 {'experience': 0.39,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.02,
  'worker': 'w_27'},
 {'experience': 0.66,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.29,
  'resilience': 0.14,
  'worker': 'w_27'},
 {'experience': 0.29,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.86,
  'resilience': 0.78,
  'worker': 'w_27'},
 {'experience': 0.67,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.4,
  'resilience': 0.1,
  'worker': 'w_27'},
 {'experience': 0.65,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.57,
  'resilience': 0.96,
  'worker': 'w_28'},
 {'experience': 0.43,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.24,
  'resilience': 0.3,
  'worker': 'w_28'},
 {'experience': 0.49,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.1,
  'resilience': 0.44,
  'worker': 'w_28'},
 {'experience': 0.93,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.88,
  'resilience': 0.87,
  'worker': 'w_28'},
 {'experience': 0.22,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.24,
  'worker': 'w_28'},
 {'experience': 0.72,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.21,
  'resilience': 0.61,
  'worker': 'w_28'},
 {'experience': 0.5,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.44,
  'resilience': 0.14,
  'worker': 'w_28'},
 {'experience': 0.35,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.13,
  'resilience': 0.3,
  'worker': 'w_28'},
 {'experience': 0.01,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.76,
  'worker': 'w_29'},
 {'experience': 0.91,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.83,
  'resilience': 0.84,
  'worker': 'w_29'},
 {'experience': 0.84,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.58,
  'resilience': 0.41,
  'worker': 'w_29'},
 {'experience': 0.53,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.67,
  'resilience': 0.85,
  'worker': 'w_29'},
 {'experience': 0.22,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.39,
  'resilience': 0.79,
  'worker': 'w_29'},
 {'experience': 0.55,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.63,
  'resilience': 0.83,
  'worker': 'w_29'},
 {'experience': 0.49,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.02,
  'resilience': 0.87,
  'worker': 'w_29'},
 {'experience': 0.45,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.51,
  'resilience': 0.28,
  'worker': 'w_29'},
 {'experience': 0.66,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.8,
  'resilience': 0.35,
  'worker': 'w_30'},
 {'experience': 0.56,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.45,
  'resilience': 0.33,
  'worker': 'w_30'},
 {'experience': 0.34,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.09,
  'resilience': 0.98,
  'worker': 'w_30'},
 {'experience': 0.52,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.39,
  'worker': 'w_30'},
 {'experience': 0.59,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.31,
  'resilience': 0.08,
  'worker': 'w_30'},
 {'experience': 0.22,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.51,
  'worker': 'w_30'},
 {'experience': 0.03,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.65,
  'resilience': 0.94,
  'worker': 'w_30'},
 {'experience': 0.2,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.55,
  'resilience': 0.85,
  'worker': 'w_30'},
 {'experience': 0.98,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.83,
  'resilience': 0.88,
  'worker': 'w_31'},
 {'experience': 0.36,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.22,
  'resilience': 0.95,
  'worker': 'w_31'},
 {'experience': 0.63,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.42,
  'resilience': 0.81,
  'worker': 'w_31'},
 {'experience': 0.27,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.82,
  'resilience': 0.36,
  'worker': 'w_31'},
 {'experience': 0.87,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.35,
  'worker': 'w_31'},
 {'experience': 0.0,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.05,
  'resilience': 0.94,
  'worker': 'w_31'},
 {'experience': 0.97,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.85,
  'worker': 'w_31'},
 {'experience': 0.44,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.93,
  'resilience': 0.3,
  'worker': 'w_31'},
 {'experience': 0.25,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.74,
  'worker': 'w_32'},
 {'experience': 0.43,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.7,
  'resilience': 0.96,
  'worker': 'w_32'},
 {'experience': 0.9,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.09,
  'resilience': 0.77,
  'worker': 'w_32'},
 {'experience': 0.09,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.36,
  'resilience': 0.39,
  'worker': 'w_32'},
 {'experience': 0.92,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.28,
  'resilience': 0.31,
  'worker': 'w_32'},
 {'experience': 0.36,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.73,
  'resilience': 0.69,
  'worker': 'w_32'},
 {'experience': 0.45,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.33,
  'resilience': 0.68,
  'worker': 'w_32'},
 {'experience': 0.21,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.26,
  'resilience': 0.8,
  'worker': 'w_32'},
 {'experience': 0.54,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.83,
  'resilience': 0.75,
  'worker': 'w_33'},
 {'experience': 0.07,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.54,
  'resilience': 0.46,
  'worker': 'w_33'},
 {'experience': 0.86,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.73,
  'resilience': 0.08,
  'worker': 'w_33'},
 {'experience': 0.47,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.69,
  'resilience': 0.0,
  'worker': 'w_33'},
 {'experience': 0.64,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.61,
  'resilience': 0.31,
  'worker': 'w_33'},
 {'experience': 0.58,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.03,
  'resilience': 0.61,
  'worker': 'w_33'},
 {'experience': 0.78,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.97,
  'resilience': 0.39,
  'worker': 'w_33'},
 {'experience': 0.58,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.48,
  'resilience': 0.15,
  'worker': 'w_33'},
 {'experience': 0.54,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.76,
  'worker': 'w_34'},
 {'experience': 0.52,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.69,
  'resilience': 0.05,
  'worker': 'w_34'},
 {'experience': 0.59,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.41,
  'resilience': 0.51,
  'worker': 'w_34'},
 {'experience': 0.5,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.17,
  'resilience': 0.16,
  'worker': 'w_34'},
 {'experience': 0.5,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.05,
  'resilience': 0.43,
  'worker': 'w_34'},
 {'experience': 0.29,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.66,
  'resilience': 0.58,
  'worker': 'w_34'},
 {'experience': 0.12,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.04,
  'resilience': 0.42,
  'worker': 'w_34'},
 {'experience': 0.8,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.69,
  'resilience': 0.87,
  'worker': 'w_34'},
 {'experience': 0.11,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.53,
  'resilience': 0.84,
  'worker': 'w_35'},
 {'experience': 0.43,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.36,
  'resilience': 0.74,
  'worker': 'w_35'},
 {'experience': 0.62,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.05,
  'worker': 'w_35'},
 {'experience': 0.38,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.5,
  'resilience': 0.45,
  'worker': 'w_35'},
 {'experience': 0.79,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.75,
  'worker': 'w_35'},
 {'experience': 0.32,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.17,
  'worker': 'w_35'},
 {'experience': 0.54,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.02,
  'resilience': 0.36,
  'worker': 'w_35'},
 {'experience': 0.43,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.89,
  'resilience': 0.77,
  'worker': 'w_35'},
 {'experience': 0.96,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.96,
  'resilience': 0.52,
  'worker': 'w_36'},
 {'experience': 0.61,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.14,
  'resilience': 0.63,
  'worker': 'w_36'},
 {'experience': 0.27,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.71,
  'resilience': 0.82,
  'worker': 'w_36'},
 {'experience': 0.9,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.25,
  'resilience': 0.81,
  'worker': 'w_36'},
 {'experience': 0.95,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.6,
  'resilience': 0.96,
  'worker': 'w_36'},
 {'experience': 0.71,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.19,
  'resilience': 0.49,
  'worker': 'w_36'},
 {'experience': 0.95,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.54,
  'resilience': 0.7,
  'worker': 'w_36'},
 {'experience': 0.64,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.19,
  'worker': 'w_36'},
 {'experience': 0.94,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.01,
  'resilience': 0.76,
  'worker': 'w_37'},
 {'experience': 0.02,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.62,
  'resilience': 0.96,
  'worker': 'w_37'},
 {'experience': 0.49,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.48,
  'resilience': 0.36,
  'worker': 'w_37'},
 {'experience': 0.53,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.24,
  'resilience': 0.4,
  'worker': 'w_37'},
 {'experience': 0.77,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.81,
  'worker': 'w_37'},
 {'experience': 0.66,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.68,
  'resilience': 0.64,
  'worker': 'w_37'},
 {'experience': 0.1,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.57,
  'resilience': 0.33,
  'worker': 'w_37'},
 {'experience': 0.92,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.87,
  'resilience': 0.74,
  'worker': 'w_37'},
 {'experience': 0.73,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.62,
  'resilience': 0.24,
  'worker': 'w_38'},
 {'experience': 0.02,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.88,
  'resilience': 0.47,
  'worker': 'w_38'},
 {'experience': 0.64,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.86,
  'resilience': 0.39,
  'worker': 'w_38'},
 {'experience': 0.55,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.2,
  'resilience': 0.0,
  'worker': 'w_38'},
 {'experience': 0.78,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.75,
  'resilience': 0.23,
  'worker': 'w_38'},
 {'experience': 0.72,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.69,
  'worker': 'w_38'},
 {'experience': 0.97,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.37,
  'resilience': 0.1,
  'worker': 'w_38'},
 {'experience': 0.39,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.62,
  'resilience': 0.68,
  'worker': 'w_38'},
 {'experience': 0.93,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.13,
  'resilience': 0.8,
  'worker': 'w_39'},
 {'experience': 0.15,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.57,
  'resilience': 0.77,
  'worker': 'w_39'},
 {'experience': 0.83,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.06,
  'resilience': 0.25,
  'worker': 'w_39'},
 {'experience': 0.07,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.52,
  'resilience': 0.39,
  'worker': 'w_39'},
 {'experience': 0.18,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.72,
  'worker': 'w_39'},
 {'experience': 0.79,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.53,
  'resilience': 0.93,
  'worker': 'w_39'},
 {'experience': 0.18,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.07,
  'resilience': 0.21,
  'worker': 'w_39'},
 {'experience': 0.34,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.35,
  'resilience': 0.57,
  'worker': 'w_39'},
 {'experience': 0.93,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.38,
  'resilience': 0.87,
  'worker': 'w_40'},
 {'experience': 0.57,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.46,
  'resilience': 0.36,
  'worker': 'w_40'},
 {'experience': 0.14,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.5,
  'resilience': 0.73,
  'worker': 'w_40'},
 {'experience': 0.08,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.94,
  'resilience': 0.36,
  'worker': 'w_40'},
 {'experience': 0.59,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.81,
  'resilience': 0.03,
  'worker': 'w_40'},
 {'experience': 0.47,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.39,
  'resilience': 0.6,
  'worker': 'w_40'},
 {'experience': 0.84,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.26,
  'resilience': 0.76,
  'worker': 'w_40'},
 {'experience': 0.19,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.68,
  'resilience': 0.68,
  'worker': 'w_40'},
 {'experience': 0.08,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.76,
  'resilience': 0.16,
  'worker': 'w_41'},
 {'experience': 0.39,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.18,
  'resilience': 0.31,
  'worker': 'w_41'},
 {'experience': 0.23,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.14,
  'resilience': 0.48,
  'worker': 'w_41'},
 {'experience': 0.58,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.67,
  'resilience': 0.18,
  'worker': 'w_41'},
 {'experience': 0.21,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.64,
  'resilience': 0.67,
  'worker': 'w_41'},
 {'experience': 0.25,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.83,
  'resilience': 0.16,
  'worker': 'w_41'},
 {'experience': 0.8,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.79,
  'resilience': 0.51,
  'worker': 'w_41'},
 {'experience': 0.92,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.37,
  'resilience': 0.12,
  'worker': 'w_41'},
 {'experience': 0.45,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.34,
  'resilience': 0.98,
  'worker': 'w_42'},
 {'experience': 0.01,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.67,
  'resilience': 0.92,
  'worker': 'w_42'},
 {'experience': 0.75,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.74,
  'resilience': 0.46,
  'worker': 'w_42'},
 {'experience': 0.28,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.9,
  'resilience': 0.69,
  'worker': 'w_42'},
 {'experience': 0.62,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.84,
  'resilience': 0.12,
  'worker': 'w_42'},
 {'experience': 0.31,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.34,
  'resilience': 0.7,
  'worker': 'w_42'},
 {'experience': 0.39,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.94,
  'resilience': 0.31,
  'worker': 'w_42'},
 {'experience': 0.6,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.56,
  'resilience': 0.65,
  'worker': 'w_42'},
 {'experience': 0.06,
  'geometry': 'Geometry 1',
  'medical_condition': 'true',
  'preference': 0.6,
  'resilience': 0.52,
  'worker': 'w_43'},
 {'experience': 0.93,
  'geometry': 'Geometry 2',
  'medical_condition': 'true',
  'preference': 0.19,
  'resilience': 0.59,
  'worker': 'w_43'},
 {'experience': 0.96,
  'geometry': 'Geometry 3',
  'medical_condition': 'true',
  'preference': 0.22,
  'resilience': 0.43,
  'worker': 'w_43'},
 {'experience': 0.13,
  'geometry': 'Geometry 4',
  'medical_condition': 'true',
  'preference': 0.4,
  'resilience': 0.12,
  'worker': 'w_43'},
 {'experience': 0.87,
  'geometry': 'Geometry 5',
  'medical_condition': 'true',
  'preference': 0.85,
  'resilience': 0.78,
  'worker': 'w_43'},
 {'experience': 0.19,
  'geometry': 'Geometry 6',
  'medical_condition': 'true',
  'preference': 0.43,
  'resilience': 0.61,
  'worker': 'w_43'},
 {'experience': 0.04,
  'geometry': 'Geometry 7',
  'medical_condition': 'true',
  'preference': 0.61,
  'resilience': 0.74,
  'worker': 'w_43'},
 {'experience': 0.31,
  'geometry': 'Geometry 8',
  'medical_condition': 'true',
  'preference': 0.99,
  'resilience': 0.53,
  'worker': 'w_43'}
]

throughput_mapping = [
    {'geometry': 'Geometry 1', 'line': 'Line 3', 'throughput': 100},
    {'geometry': 'Geometry 2', 'line': 'Line 1', 'throughput': 100},
    {'geometry': 'Geometry 2', 'line': 'Line 3', 'throughput': 100},
    {'geometry': 'Geometry 3', 'line': 'Line 2', 'throughput': 100},
    {'geometry': 'Geometry 3', 'line': 'Line 3', 'throughput': 100},
    {'geometry': 'Geometry 4', 'line': 'Line 1', 'throughput': 100},
    {'geometry': 'Geometry 4', 'line': 'Line 2', 'throughput': 100},
    {'geometry': 'Geometry 5', 'line': 'Line 2', 'throughput': 100},
    {'geometry': 'Geometry 5', 'line': 'Line 3', 'throughput': 100},
    {'geometry': 'Geometry 6', 'line': 'Line 2', 'throughput': 100},
    {'geometry': 'Geometry 7', 'line': 'Line 1', 'throughput': 100},
    {'geometry': 'Geometry 8', 'line': 'Line 1', 'throughput': 100},
    {'geometry': 'Geometry 8', 'line': 'Line 3', 'throughput': 100},
]

order_data = [
    {
        'amount': 100 * 19,
        'deadline': solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
        'geometry': 'Geometry 1',
        'mold': 4,
        'order': 'Order 1',
        'priority': 'false'
    },
    {
        'amount': 100 * 7,
        'deadline': solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
        'geometry': 'Geometry 6',
        'mold': 4,
        'order': 'Order 1',
        'priority': 'false'
    },
    {
        'amount': 100 * 15,
        'deadline': solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
        'geometry': 'Geometry 7',
        'mold': 4,
        'order': 'Order 1',
        'priority': 'false'
    },
    {
        'amount': 100 * 7,
        'deadline': solver_time_to_timestamp(solver_time=480, start_timestamp=start_timestamp),
        'geometry': 'Geometry 2',
        'mold': 4,
        'order': 'Order 2',
        'priority': 'false'
    },
    {
        'amount': 100 * 23,
        'deadline': solver_time_to_timestamp(solver_time=480, start_timestamp=start_timestamp),
        'geometry': 'Geometry 8',
        'mold': 4,
        'order': 'Order 2',
        'priority': 'false'
    },
    {
        'amount': 100 * 11,
        'deadline': solver_time_to_timestamp(solver_time=1920, start_timestamp=start_timestamp),
        'geometry': 'Geometry 3',
        'mold': 4,
        'order': 'Order 3',
        'priority': 'false'
    },
    {
        'amount': 100 * 15,
        'deadline': solver_time_to_timestamp(solver_time=900, start_timestamp=start_timestamp),
        'geometry': 'Geometry 4',
        'mold': 4,
        'order': 'Order 4',
        'priority': 'false'
    },
    {
        'amount': 100 * 15,
        'deadline': solver_time_to_timestamp(solver_time=2000, start_timestamp=start_timestamp),
        'geometry': 'Geometry 5',
        'mold': 4,
        'order': 'Order 5',
        'priority': 'false'
    },
]

worker_decision_variables = namedtuple(
    'WorkerVars',
    ['available', 'medical_condition', 'preference', 'resilience', 'experience', 'allocated']
)


def get_throughput(
        throughput_mapping: list[dict[str, Any]],
        geometry: str,
        line: str,
        fallback_throughput: int = 300
):
    throughput_in_units_per_hour = None
    for elem in throughput_mapping:
        if elem["line"] == line and elem["geometry"] == geometry:
            throughput_in_units_per_hour = elem["throughput"]
            break
    if throughput_in_units_per_hour is None:
        throughput_in_units_per_hour = fallback_throughput
    if throughput_in_units_per_hour == 0:
        throughput_in_units_per_hour = fallback_throughput

    return throughput_in_units_per_hour


def get_task_total_amount_mapping(
        order_data: list,
) -> dict:
    mapping = {}
    for order_data_elem in order_data:
        geometry = order_data_elem["geometry"]
        order = order_data_elem["order"]
        amount = order_data_elem["amount"]
        mapping[f"{order} × {geometry}"] = amount
    return mapping


def get_setuptime_mapping(
        geometry_line_mapping: list,
        order_data: list,
) -> dict:
    # create a lookup table for geometry to lines and geometry to primary line
    geometry_to_lines_lookup_dict = {}

    for elem in geometry_line_mapping:
        geometry = elem["geometry"]
        main_line = elem["main_line"]
        lines = [main_line] + [
            line for line
            in elem["alternative_lines"]
            if line not in ["", "NA", "pomigl."]
        ]
        geometry_to_lines_lookup_dict[geometry] = lines
        # geometry_main_line_lookup_dict[geometry] = main_line

    def _get_lines_for_geometry(geometry: str):
        try:
            return geometry_to_lines_lookup_dict[geometry]
        except KeyError:
            return []

    # look up all possible lines to produce an order_data element
    temp = []
    for order_data_elem in order_data:
        geometry = order_data_elem["geometry"]
        possible_lines = _get_lines_for_geometry(geometry)
        for line in possible_lines:
            temp.append(order_data_elem | {"line": line})

    # create Task field. Task is the pair of order and geometry
    # This is basically the 'order' for the solver. The cp solver assumes that there is only one geometry per order,
    # which turn out to not be the case. By introducing the 'Task' can omit rewriting the solver.
    set_uptime_map_in_minutes = {}
    for elem in temp:
        elem["Task"] = f"{elem['order']} × {elem['geometry']}"
        set_uptime_map_in_minutes[elem["Task"]] = elem["mold"] * 15  # 15 minutes per mold

    return set_uptime_map_in_minutes


def validate_interval_data(data):
    task = data['Task']
    interval_start = data['Start']
    interval_end = data['Finish']
    interval_duration = data['_interval_duration']
    _setup_time = data['_setup_time']
    _setup_time_within_timebox = data['_setup_time_within_timebox']
    _is_completely_setup = data['_is_completely_setup']
    is_setup_timebox = data['is_setup_timebox']
    produced_amount = data['produced_amount']
    produced_amount_until_now = data['produced_until_now']
    total_amount = data['total_amount']

    # Assert interval start and end times
    assert interval_start < interval_end, "Interval start must be less than interval end"
    assert interval_duration > 0, "Interval duration must be greater than 0"
    assert interval_duration == interval_end - interval_start, "Interval duration must match the difference between start and end"

    # Assert setup time consistency
    assert _setup_time >= _setup_time_within_timebox, "_setup_time must be greater than or equal to _setup_time_within_timebox"
    assert _setup_time_within_timebox >= 0, "_setup_time_within_timebox must be non-negative"

    if _is_completely_setup:
        assert is_setup_timebox == 1
        assert interval_duration == _setup_time_within_timebox, "If completely setup, interval duration must equal setup time within timebox"
        assert produced_amount == 0, "If completely setup, produced amount must be 0"
        assert produced_amount_until_now == 0, "If completely setup, produced amount until now must be 0"

    if is_setup_timebox and not _is_completely_setup:
        assert produced_amount > 0, "If setup timebox, produced amount must be greater than 0"
        assert produced_amount_until_now == produced_amount, "If setup timebox, produced amount until now must equal produced amount"
        assert total_amount >= produced_amount_until_now, "Total amount must be greater than or equal to produced amount until now"

    if not is_setup_timebox:
        assert produced_amount > 0, "If not setup timebox, produced amount must be greater than 0"
        assert produced_amount_until_now <= total_amount, "Produced amount until now must be less than or equal to total amount"


class CrfWorkerAllocationEnv(gym.Env):
    metadata = {
        'render.modes': ['human'],
    }

    # attributes (will be initialized in the constructor)
    # _state: pd.DataFrame
    # _initial_state: pd.DataFrame

    def __init__(self, *,
                 previous_step_output: list[dict[str, Any]],
                 worker_availabilities: list[dict[str, Any]],
                 geometry_line_mapping: list[dict[str, Any]],
                 human_factor_data: dict[str, Any],
                 order_data: list[dict[str, Any]],
                 start_timestamp: int,
                 dense_reward: bool = True,
                 preference_weight: float = 1.0,
                 resilience_weight: float = 1.0,
                 experience_weight: float = 1.0,
                 allocate_workers_on_the_same_line_if_possible: bool = True,
                 throughput_mapping: list[dict[str, Any]] = throughput_mapping,
                 ):

        df_state = self._init_state_dataframe(
            previous_step_output=previous_step_output,
            worker_availabilities=worker_availabilities,
            geometry_line_mapping=geometry_line_mapping,
            human_factor_data=human_factor_data,
            start_timestamp=start_timestamp,
            order_data=order_data,
        )

        # will be set by load_state
        self._state: pd.DataFrame = None
        self._worker_to_idx_map: dict = None
        self._idx_to_worker_map: dict = None
        self._n_rows: int = None
        self._n_workers: int = None

        # reward settings
        self._dense_reward = dense_reward
        self._preference_weight = preference_weight
        self._resilience_weight = resilience_weight
        self._experience_weight = experience_weight

        self._start_timestamp = start_timestamp

        self._allocate_workers_on_the_same_line_if_possible = allocate_workers_on_the_same_line_if_possible

        self._initial_state = df_state.copy(deep=True)

        self.load_state(state=self._initial_state)

        # the action space is basically a matrix of workers and the rows of the df, but flattened
        # action = 0 is the first worker for the first row
        # action = 1 is the second worker for the first row
        # action = _n_workers + 0 is the first worker for the second row
        # action = _n_workers + 1 is the second worker for the second row
        # ...
        # action = _n_workers * _n_rows - 1 is the last worker for the last row
        possible_actions = self._n_workers * self._n_rows

        self._no_action_falg = False if possible_actions > 0 else True
        self.action_space = gym.spaces.Discrete(possible_actions if possible_actions > 0 else 1)

        initial_observation = self._state_as_numpy_array()
        self.observation_space = gym.spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=initial_observation.shape,
            dtype=initial_observation.dtype
        )


    def action_tuple_to_action_idx(self, action_tuple: tuple[int, int]) -> int:
        row, worker = action_tuple
        return row * self._n_workers + worker

    def action_idx_to_action_tuple(self, action_idx: int) -> tuple[int, int]:
        return divmod(action_idx, self._n_workers)

    @staticmethod
    def _init_state_dataframe(previous_step_output: list[dict[str, Any]],
                              worker_availabilities: list[dict[str, Any]],
                              geometry_line_mapping: list[dict[str, Any]],
                              human_factor_data: dict[str, Any],
                              start_timestamp: int,
                              order_data: list[dict[str, Any]],
                              ) -> pd.DataFrame:

        line_allocations = previous_step_output

        # map line allocation to solver time domain
        line_allocations = [
            elem | {
                'Start_solver_time': timestamp_to_solver_time(elem['Start'], start_timestamp),
                'Finish_solver_time': timestamp_to_solver_time(elem['Finish'], start_timestamp),
            } for elem in line_allocations
        ]
        # print(f"line_allocations: {pprint.pformat(line_allocations)}")

        # map worker availabilities to solver time domain
        worker_availabilities = [
            elem | {
                'from_solver_time': timestamp_to_solver_time(elem['from_timestamp'], start_timestamp),
                'end_solver_time': timestamp_to_solver_time(elem['end_timestamp'], start_timestamp),
            } for elem in worker_availabilities
        ]
        # print(f"worker_availabilities: {pprint.pformat(worker_availabilities)}")

        relevant_intervals = CrfWorkerAllocationEnv._get_relevant_intervals(
            line_allocations=line_allocations,
            worker_availabilities=worker_availabilities,
            start_timestamp=start_timestamp,
        )
        # print(f"relevant_intervals: {relevant_intervals}")

        # get mapping for setup time
        setup_time_mapping = get_setuptime_mapping(
            geometry_line_mapping=geometry_line_mapping,
            order_data=order_data,
        )
        # remaining setup time
        remaining_setup_time_mapping = copy.deepcopy(setup_time_mapping)

        # get mapping for task total amount
        task_total_amount_mapping = get_task_total_amount_mapping(
            order_data=order_data,
        )

        # produced amount mapping
        # maps the same key as task_total_amount_mapping
        # it will be incremented by the produced amount while in the loops below
        produced_amount_mapping = {}
        for key in task_total_amount_mapping.keys():
            produced_amount_mapping[key] = 0

        df_data = []
        for interval_idx, (interval_start, interval_end) in enumerate(relevant_intervals):
            line_allocations_within_interval = [
                elem for elem in line_allocations
                if
                CrfWorkerAllocationEnv._intervals_overlap(
                    (elem['Start_solver_time'], elem['Finish_solver_time']),
                    (interval_start, interval_end)
                )
            ]
            workers_within_interval = [
                elem for elem in worker_availabilities
                if
                CrfWorkerAllocationEnv._intervals_overlap(
                    (elem['from_solver_time'], elem['end_solver_time']),
                    (interval_start, interval_end)
                )
            ]

            for line_elem in line_allocations_within_interval:

                required_workers = CrfWorkerAllocationEnv._get_required_number_of_workers(
                    line=line_elem['Resource'],
                    geometry=line_elem['geometry'],
                    geometry_line_mapping=geometry_line_mapping
                )
                log.debug(f"required workers for line '{line_elem['Resource']}' and geometry '{line_elem['geometry']}' "
                          f"is {required_workers}.")

                interval_duration = interval_end - interval_start

                task = line_elem['Task']

                _is_completely_setup = 0
                _setup_time_within_timebox = -1
                production_time = -1
                produced_amount = 0

                _setup_time = setup_time_mapping[line_elem['Task']]

                # check if the interval is used for setup
                if remaining_setup_time_mapping[task] > 0:
                    is_setup_timebox = 1

                    # decrease the remaining setup time
                    remaining_setup_time_mapping[task] -= interval_duration

                    if remaining_setup_time_mapping[task] >= 0:
                        _is_completely_setup = 1
                        _setup_time_within_timebox = interval_duration

                        assert _setup_time >= _setup_time_within_timebox
                        assert _setup_time_within_timebox >= 0

                        production_time = 0
                    else:
                        # note that remaining_setup_time_mapping[task] is negative
                        _setup_time_within_timebox = interval_duration + remaining_setup_time_mapping[task]
                        #print(f"setup time: {_setup_time}")
                        #print(
                        #   f"setup time within timebox: {_setup_time_within_timebox}, remaining setup time: {remaining_setup_time_mapping[task]}, interval duration: {interval_duration}")
                        assert _setup_time_within_timebox >= 0
                        assert _setup_time >= _setup_time_within_timebox

                        production_time = interval_duration - _setup_time_within_timebox

                        assert production_time <= interval_duration

                else:
                    is_setup_timebox = 0
                    _setup_time_within_timebox = 0
                    production_time = interval_duration

                    assert _setup_time >= _setup_time_within_timebox
                    assert _setup_time_within_timebox >= 0

                if _is_completely_setup == 0:
                    if is_setup_timebox:
                        assert _setup_time_within_timebox < interval_duration
                        assert production_time < interval_duration, f"production time should be smaller than interval duration, " \
                                                                    f"but is {production_time} >= {interval_duration} for task {task} and interval {interval_idx}"
                    else:
                        assert _setup_time_within_timebox == 0
                        assert production_time == interval_duration, f"production time should be equal to interval duration, " \
                                                                     f"but is {production_time} != {interval_duration} for task {task} and interval {interval_idx}"
                else:
                    assert _setup_time_within_timebox == interval_duration
                    assert production_time == 0, f"production time should be 0, but is {production_time} for task {task} and interval {interval_idx}"

                throughput_in_units_per_hour = get_throughput(
                    throughput_mapping=throughput_mapping,
                    geometry=line_elem['geometry'],
                    line=line_elem['Resource'],
                )
                if production_time:

                    assert throughput_in_units_per_hour > 0
                    # note: production time is in minutes
                    produced_amount = throughput_in_units_per_hour * production_time / 60
                    assert produced_amount >= 0
                    #assert produced_amount <= task_total_amount_mapping[task]
                else:
                    produced_amount = 0

                if _is_completely_setup == 1:
                    assert produced_amount == 0, f"produced amount should be 0, but is {produced_amount} for task {task} and interval {interval_idx}"
                else:
                    assert produced_amount > 0, f"produced amount should be greater than 0, but is {produced_amount} for task {task} and interval {interval_idx}"

                if is_setup_timebox == 1:
                    assert produced_amount < throughput_in_units_per_hour * interval_duration / 60

                # increment the produced amount mapping
                produced_amount_mapping[task] += produced_amount

                produced_amount_until_now = produced_amount_mapping[task]

                assert produced_amount_until_now <= task_total_amount_mapping[task], \
                    f"produced amount until now should be smaller than total amount, " \
                    f"but is {produced_amount_until_now} > {task_total_amount_mapping[task]} for task {task} and interval {interval_idx}"

                assert produced_amount_until_now >= produced_amount, \
                    f"produced amount until now should be greater than produced amount, " \
                    f"but is {produced_amount_until_now} < {produced_amount} for task {task} and interval {interval_idx}"

                if _is_completely_setup == 1:
                    assert produced_amount == 0, f"produced amount should be 0, but is {produced_amount} for task {task} and interval {interval_idx}"
                    assert produced_amount_until_now == 0, \
                        f"produced amount until now should be 0, " \
                        f"but is {produced_amount_until_now} for task {task} and interval {interval_idx}"
                    assert interval_duration == _setup_time_within_timebox
                else:
                    assert produced_amount > 0, f"produced amount should be greater than 0, but is {produced_amount} for task {task} and interval {interval_idx}"
                    if is_setup_timebox == 1:
                        assert produced_amount == produced_amount_until_now, \
                            f"produced amount should be equal to produced amount until now, " \
                            f"but is {produced_amount} != {produced_amount_until_now} for task {task} and interval {interval_idx}"

                if _setup_time_within_timebox > 0:
                    assert is_setup_timebox == 1, f"setup time within timebox should be greater than 0, " \
                                                  f"but is {_setup_time_within_timebox} for task {task} and interval {interval_idx}"
                    assert _setup_time_within_timebox <= interval_duration, \
                        f"setup time within timebox should be smaller than interval duration, " \
                        f"but is {_setup_time_within_timebox} > {interval_duration} for task {task} and interval {interval_idx}"
                    assert produced_amount_until_now == produced_amount

                assert interval_start < interval_end
                assert interval_duration > 0
                assert interval_duration == interval_end - interval_start

                data_row_dict = {
                    'interval_no': interval_idx,
                    'is_current_interval': 1 if interval_idx == 0 else 0,
                    'interval_start': interval_start,
                    'Start': interval_start,
                    'interval_end': interval_end,
                    'Finish': interval_end,
                    'Task': line_elem['Task'],
                    'Task_interval': f"{line_elem['Task']} × Interval {interval_idx}",
                    # todo: check if timstamp is needed instead of interval idx
                    'line': line_elem['Resource'],
                    'geometry': line_elem['geometry'],
                    'required_workers': required_workers,
                    'allocated_workers': 0,
                    'required_workers_met': 0,
                    'row_done': 0,

                    'total_amount': task_total_amount_mapping[line_elem['Task']],
                    'produced_amount': produced_amount,
                    'produced_until_now': produced_amount_until_now,
                    # 'timebox_amount': 0, # Note: this will be calculated once the environment is solved
                    '_setup_time': _setup_time,  # the overall setup time (all timeboxes summed up)
                    '_interval_duration': interval_duration,  # the duration of this timebox
                    '_setup_time_within_timebox': _setup_time_within_timebox,  # the setup time within this timebox
                    '_is_completely_setup': _is_completely_setup,
                    'is_setup_timebox': is_setup_timebox,
                }
                # validate_interval_data(data=data_row_dict)
                n_workers_available_for_this_task = 0
                for worker_in_interval in workers_within_interval:
                    worker_id = worker_in_interval['worker']
                    preference, resilience, medical_condition, experience = CrfWorkerAllocationEnv._get_human_factor_data(
                        worker=worker_id,
                        geometry=line_elem['geometry'],
                        human_factor_data=human_factor_data
                    )
                    n_workers_available_for_this_task += 1
                    res = worker_decision_variables(
                        available=1,
                        medical_condition=int(medical_condition),
                        preference=preference,
                        resilience=resilience,
                        experience=experience,
                        allocated=0
                    )
                    data_row_dict = data_row_dict | {
                        f'worker_{worker_id}': res
                    }

                df_data.append(data_row_dict)

        # print(f"line_allocations after processing: {pprint.pformat(df_data)}")
        df = pd.DataFrame(df_data)

        # sanity checks

        # check if "_setup_time_within_timebox" adds up to "_setup_time" for each task
        for task in setup_time_mapping.keys():
            assert df[df['Task'] == task]['_setup_time_within_timebox'].sum() == setup_time_mapping[task], \
                f"setup time within timebox does not add up to setup time for task {task}"

        # check if "produced_amount" adds up to "total_amount" for each task
        # only check task present in the dataframe:
        tasks = df['Task'].unique()
        for task in tasks:
            ratio = df[df['Task'] == task]['produced_amount'].sum() / task_total_amount_mapping[task]
            log.debug(
                f"produced amount for task '{task}': {df[df['Task'] == task]['produced_amount'].sum()}, expected: {task_total_amount_mapping[task]}, ratio: {ratio}"
            )
            # print(f"produced amount for task '{task}': {df[df['Task'] == task]['produced_amount'].sum()}, expected: {task_total_amount_mapping[task]}, ratio: {ratio}")
            assert df[df['Task'] == task]['produced_amount'].sum() == task_total_amount_mapping[task], \
                f"produced amount does not add up to total amount for task {task}"

        return df

    @staticmethod
    def _get_human_factor_data(
            worker: str,
            geometry: str,
            human_factor_data: dict[str, Any]) -> (float, float, bool, float):
        for elem in human_factor_data:
            if elem['geometry'] == geometry and elem['worker'] == worker:
                preference = elem['preference']
                resilience = elem['resilience']
                medical_condition = elem['medical_condition'].lower() == "true"
                experience = elem['experience']

                log.debug(
                    f"human factor data found for worker '{worker}' and geometry '{geometry}': {(preference, resilience, medical_condition, experience)}")

                return preference, resilience, medical_condition, experience
        else:
            log.warning(f"no human factor data found for worker '{worker}' and geometry '{geometry}'. "
                        f"Returning default values of (0.5, 0.5, True, 0.5)")
            return 0.5, 0.5, True, 0.5

    @staticmethod
    def _human_readable_timestamp(timestamp: int | float) -> str:
        human_readable_time = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        return human_readable_time.strftime('%A %Y-%m-%d %H:%M:%S')

    @staticmethod
    def _get_required_number_of_workers(line: str, geometry: str, geometry_line_mapping: list[dict[str, Any]]) -> int:
        for elem in geometry_line_mapping:
            if elem['geometry'] == geometry:
                if elem['main_line'] == line or line in elem['alternative_lines']:
                    return elem['number_of_workers']
        else:
            log.warning(f"no line allocation found for line {line} and geo {geometry}. Returning a default value of 4")
            return 4

    @staticmethod
    def _intervals_overlap(interval1, interval2):
        start1, end1 = interval1
        start2, end2 = interval2
        return start1 < end2 and start2 < end1

    @staticmethod
    def _get_relevant_intervals(
            line_allocations: list[dict],
            worker_availabilities: list[dict],
            start_timestamp: int) -> list[tuple[int, int]]:

        interval_bounds = set()

        for line_allocation_elem in line_allocations:
            interval_bounds.add(line_allocation_elem['Start_solver_time'])
            interval_bounds.add(line_allocation_elem['Finish_solver_time'])

        for worker_availabilities_elem in worker_availabilities:
            interval_bounds.add(worker_availabilities_elem['from_solver_time'])
            interval_bounds.add(worker_availabilities_elem['end_solver_time'])

        interval_bounds_ascending_list = sorted(list(interval_bounds))

        log.debug(f"interval bounds (solver time): {interval_bounds_ascending_list}")

        interval_tuple = []  # [(start, end), ...]
        for interval_start, interval_end in zip(interval_bounds_ascending_list[:],  # all but the last element
                                                interval_bounds_ascending_list[1:]):
            interval_tuple.append((interval_start, interval_end))

        log.debug(f"interval tuple (solver time): {interval_tuple}")

        n_intervals = len(interval_tuple)
        log.debug(f"the schedule is devided into {n_intervals} intervals: {interval_tuple}")

        relevant_intervals = []

        for interval_idx, (interval_start, interval_end) in enumerate(interval_tuple):
            log.debug(f"interval {interval_idx}: {interval_start} - {interval_end} "
                      f"({solver_time_to_timestamp(interval_start, start_timestamp)}-{solver_time_to_timestamp(interval_end, start_timestamp)})")
            log.debug(f"interval {interval_idx}: "
                      f"{CrfWorkerAllocationEnv._human_readable_timestamp(solver_time_to_timestamp(interval_start, start_timestamp))} "
                      f"- {CrfWorkerAllocationEnv._human_readable_timestamp(solver_time_to_timestamp(interval_end, start_timestamp))}")
            # find line allocations that are within this interval
            line_allocations_within_interval = [
                elem for elem in line_allocations
                if
                CrfWorkerAllocationEnv._intervals_overlap((elem['Start_solver_time'], elem['Finish_solver_time']),
                                                          (interval_start, interval_end))
            ]
            log.debug("line allocations within this interval:")
            log.debug(pprint.pformat(line_allocations_within_interval))
            log.debug("available workers within this interval:")

            workers_within_interval = [
                elem for elem in worker_availabilities
                if
                CrfWorkerAllocationEnv._intervals_overlap(
                    (elem['from_solver_time'], elem['end_solver_time']),
                    (interval_start, interval_end)
                )
            ]
            log.debug(pprint.pformat(workers_within_interval))

            # if len(workers_within_interval) and len(line_allocations_within_interval):
            # only finter based on line allocations. that way total amount adds up to the correct value
            if len(line_allocations_within_interval):
                relevant_intervals.append((interval_start, interval_end))

        return relevant_intervals

    def get_state(self) -> pd.DataFrame:
        return self._state

    def load_state(self, state: pd.DataFrame) -> None:
        # this method does not check if the state is valid or not
        # todo: add a check for the validity of the state
        self._state = state.copy(deep=True)

        worker_to_idx_map = {col: idx for idx, col in enumerate(sorted(self._get_all_worker_cols()))}
        idx_to_worker_map = {idx: col for col, idx in worker_to_idx_map.items()}

        self._worker_to_idx_map = worker_to_idx_map
        self._idx_to_worker_map = idx_to_worker_map

        self._n_rows = self._state.shape[0]
        self._n_workers = len(worker_to_idx_map)

    def _calculate_reward(
            self,
            named_worker_tuple: tuple,
            is_terminal: bool,
            row_done_without_meeting_required_workers_penalty=0
    ) -> SupportsFloat:
        if self._dense_reward:
            reward = 0
            reward += self._preference_weight * named_worker_tuple.preference
            reward += self._resilience_weight * named_worker_tuple.resilience
            reward += self._experience_weight * named_worker_tuple.experience
            reward += row_done_without_meeting_required_workers_penalty
            return reward
        else:
            if is_terminal:
                experience, resilience, preference = env.get_KPIs()
                weighted_sum = self._preference_weight * preference + self._resilience_weight * resilience + self._experience_weight * experience
                scaled_weighted_sum = weighted_sum / (
                        self._preference_weight + self._resilience_weight + self._experience_weight)
                return scaled_weighted_sum
            else:
                return 0

    def step(self, action: int) -> (npt.NDArray, SupportsFloat, bool, bool, dict[str, Any]):
        if self._no_action_falg == True:
            return self._state_as_numpy_array(), 0, True, False, {}
        # convert action to tuple
        action_row, action_worker = self.action_idx_to_action_tuple(action)
        # log.info(f"performing action: {action} ({action_row}, {action_worker})")

        # check if the action is valid
        # 1. 'is_current_interval' has to be 1 in the specified row
        # 2.  the worker has to be available and not allocated and has to have a medical condition of True
        if self._state.at[action_row, 'is_current_interval'] == 0:
            # log.warning(f"the interval is not current, so the action is invalid.")
            return self._state_as_numpy_array(), 0, self.is_terminal_state(), False, {}

        # if the row is done, then the action is invalid
        if self._state.at[action_row, 'row_done'] == 1:
            # log.warning(f"the row is done, so the action is invalid.")
            return self._state_as_numpy_array(), 0, self.is_terminal_state(), False, {}

        worker_col = self._idx_to_worker_map[action_worker]

        # worker_decision_variables = namedtuple(
        #     'WorkerVars',
        #     ['available', 'medical_condition', 'preference', 'resilience', 'experience', 'allocated']
        # )
        worker_named_tuple = self._state.at[action_row, worker_col]

        if not isinstance(worker_named_tuple, tuple):
            # log.warning(f"the worker is not available, so the action is invalid.")
            return self._state_as_numpy_array(), 0, self.is_terminal_state(), False, {}

        if not worker_named_tuple.available:
            # log.warning(f"the worker is not available, so the action is invalid.")
            return self._state_as_numpy_array(), 0, self.is_terminal_state(), False, {}

        if worker_named_tuple.allocated:
            # log.warning(f"the worker is already allocated, so the action is invalid.")
            return self._state_as_numpy_array(), 0, self.is_terminal_state(), False, {}

        if not worker_named_tuple.medical_condition:
            # log.warning(f"the worker has a medical condition of False, so the action is invalid.")
            return self._state_as_numpy_array(), 0, self.is_terminal_state(), False, {}

        # at this point, the action is valid
        # so we can update the state

        # 1.1 set the worker as allocated in the specified row
        # 1.2 update the allocated_workers and required_workers_met in the specified row
        #    -> required_workers_met is 1 if the allocated_workers == required_workers
        #    -> allocated_workers is incremented by 1
        # 1.3 set the worker as not available in all rows with is_current_interval == 1
        # 1.4 check if the action_row is done
        #    -> if required_workers_met == 1, then set row_done to 1
        #    -> if there are no more workers available for this row, then set row_done to 1

        # 1.1
        named_tuple = self._state.at[action_row, worker_col]
        allocated_updated_named_tuple = named_tuple._replace(allocated=1)
        self._state.at[action_row, worker_col] = allocated_updated_named_tuple
        # 1.2
        self._state.at[action_row, 'allocated_workers'] += 1
        row_done_without_meeting_required_workers_penalty = 0
        if self._state.at[action_row, 'allocated_workers'] == self._state.at[action_row, 'required_workers']:
            self._state.at[action_row, 'required_workers_met'] = 1
            # 1.4
            self._state.at[action_row, 'row_done'] = 1
        else:
            valid_actions_for_row = self.valid_action_tuples_for_row(action_row)
            if not len(valid_actions_for_row):
                self._state.at[action_row, 'row_done'] = 1
                row_done_without_meeting_required_workers_penalty = -10
            # check also for the remaining rows with is_current_interval == 1 and row_done == 0 if there are no more
            # workers available
            for row_idx, row in self._state.iterrows():
                if row['is_current_interval'] == 1:
                    valid_actions_for_row = self.valid_action_tuples_for_row(row_idx)
                    if not len(valid_actions_for_row):
                        self._state.at[row_idx, 'row_done'] = 1
                        row_done_without_meeting_required_workers_penalty = -10

        # 1.3
        for row_idx, action_row in self._state.iterrows():
            if action_row['is_current_interval'] == 1:
                named_tuple = self._state.at[row_idx, worker_col]
                updated_named_tuple = named_tuple._replace(available=0)
                self._state.at[row_idx, worker_col] = updated_named_tuple

        # check if we need to go to the next interval
        # we go to the next interval if all rows where is_current_interval == 1 are done, i.e. row_done == 1
        goto_next_interval = self._state[self._state['is_current_interval'] == 1]['row_done'].all()
        # log.info(f"goto_next_interval: {goto_next_interval}")

        nested_reward = 0

        # todo: find a more elegant way for going to the next interval in the terminal corner case
        all_rows_done = True
        for row_idx, row in self.get_state().iterrows():
            if row['is_current_interval'] == 1:
                valid_actions = self.valid_action_tuples_for_row(row_idx)
                # log.info(f"valid actions for row {row_idx}: {valid_actions}")
                if len(valid_actions):
                    all_rows_done = False
                    break
        if all_rows_done:
            goto_next_interval = True

        # if valid action mask has only zeros, then goto_next_interval
        if not self.valid_action_mask().any():
            goto_next_interval = True

        if goto_next_interval:
            prev_interval_no = self._state[self._state['is_current_interval'] == 1]['interval_no'].iloc[0]
            next_interval_no = prev_interval_no + 1

            # log.info(f"transitioning form interval {prev_interval_no} to interval {next_interval_no}.")

            # set all rows with interval_no == current_interval_no to is_current_interval = 0
            self._state.loc[self._state['interval_no'] == prev_interval_no, 'is_current_interval'] = 0

            # set all rows with interval_no == next_interval_no to is_current_interval = 1
            self._state.loc[self._state['interval_no'] == next_interval_no, 'is_current_interval'] = 1

            # check if we are in the last interval
            if self.is_terminal_state():
                # print("terminal state reached")
                reward = self._calculate_reward(
                    named_worker_tuple=allocated_updated_named_tuple,
                    is_terminal=True,
                    row_done_without_meeting_required_workers_penalty=row_done_without_meeting_required_workers_penalty
                )
                return self._state_as_numpy_array(), nested_reward + reward, True, False, {}

            if self._allocate_workers_on_the_same_line_if_possible:
                # if
                # - a worker was allocated (named_tuple.allocated == 1) in a row with interval_no == prev_interval_no to a Task specific T
                # - and the worker is available a row with is_current_interval == 1 for the same Task T
                # then set the worker as not available in all rows with is_current_interval == 1
                # and set the worker as allocated in the row with is_current_interval == 1 and Task == T

                for prev_row_idx, prev_row in self._state.iterrows():
                    if prev_row['interval_no'] != prev_interval_no:
                        continue
                    # row where interval_no == prev_interval_no

                    for next_row_idx, next_row in self._state.iterrows():
                        if next_row['interval_no'] != next_interval_no:
                            continue
                        # row where interval_no == next_interval_no

                        if next_row['interval_no'] != next_interval_no:
                            continue

                        if prev_row['Task'] != next_row['Task']:
                            continue

                        task_candidate = next_row_idx
                        worker_candidate = self._get_allocated_workers_in_row(prev_row_idx)

                        # create tuples with task_candidate as the first element and worker_candidate entries
                        # as the second element
                        candidates = [(task_candidate, worker) for worker in worker_candidate]

                        # perform a step with all candidates
                        # not valid states will be ignored by the step function anyway, so there is no need to check
                        for candidate_tuple in candidates:
                            candidate_action = self.action_tuple_to_action_idx(candidate_tuple)
                            _, rew, _, _, _ = self.step(candidate_action)
                            nested_reward += rew

        is_terminal = self.is_terminal_state()
        reward = self._calculate_reward(
            named_worker_tuple=allocated_updated_named_tuple,
            is_terminal=is_terminal,
            row_done_without_meeting_required_workers_penalty=row_done_without_meeting_required_workers_penalty
        )

        # validate all rows of the state dict with validate_interval_data(
        #                     data=data_row_dict
        #                 )
        #for row_idx, row in self._state.iterrows():
        #   validate_interval_data(data=row.to_dict())

        return self._state_as_numpy_array(), nested_reward + reward, is_terminal, False, {}

    def reset(self, **kwargs) -> (npt.NDArray, dict[str, Any]):
        super().reset(**kwargs)
        self.load_state(state=self._initial_state)
        return self._state_as_numpy_array(), {}

    def _get_all_worker_cols(self) -> list[str]:
        worker_ids = []
        for col in self._state.columns:
            if col.startswith('worker_'):
                worker_ids.append(col)
        return worker_ids

    def _get_number_of_workers(self) -> int:
        return len(self._get_all_worker_cols())

    def _state_as_numpy_array(self) -> npt.NDArray:
        df = self.get_state().copy()

        # remove columns that are not needed
        df = df.drop(columns=[
            'Task',
            'Task_interval',
            'line',
            'geometry',
            'interval_no',
        ])

        # tuple_columns = [col for col in df.columns if df[col].dtype == 'object']
        tuple_columns = self._get_all_worker_cols()

        # fill NaN values with (1, 1, 0.22, 0.93, 0.56, 0)
        for col in tuple_columns:
            df[col] = df[col].fillna(
                pd.Series([
                    (0, 0, 0, 0, 0, 0)
                    for _ in range(df.shape[0])
                ])
            )

        # Expand each tuple column into separate columns
        for col in tuple_columns:
            expanded_df = pd.concat([pd.Series(v) for v in df[col]], axis=1).T
            expanded_df.columns = [f'{col}_{i}' for i in range(expanded_df.shape[1])]
            df = pd.concat([df.drop(columns=[col]), expanded_df], axis=1)

        # Convert the final DataFrame to a NumPy array
        return df.to_numpy(dtype=np.float32)

    def render(self, mode='human', **kwargs) -> Any:
        df = self.get_state().copy()
        # add index of the row as a column
        df['idx'] = df.index
        df = df[df['is_current_interval'] == 1]
        # print(f"all cols: {df.columns.tolist()}")
        renaming_dict = {
            'interval_start': 'i_start',
            'interval_end': 'i_end',
            'required_workers': 'req_w',
            'allocated_workers': 'alloc_w',
            'required_workers_met': 'req_w_met',
            'interval_no': 'no.',
        }
        cols_to_display = [
            'idx',
            'no.',
            'i_start',
            'i_end',
            'line',
            # 'geometry',
            'req_w',
            'alloc_w',
            'req_w_met',
        ]
        # renaming cols, so more fit into the console
        df = df.rename(columns=renaming_dict)
        # add worker columns that have not NaN values to cols_to_display
        n_visualized = 0
        for col in df.columns:
            if col.startswith('worker_') and not df[col].isna().all():
                cols_to_display.append(col)
                n_visualized += 1
            if n_visualized >= 4:
                break
        # print(self._worker_to_idx_map)
        print(df[cols_to_display].to_string())

    def is_terminal_state(self) -> bool:
        if self._no_action_falg:
            return True
        # the terminal state is reached when is_current_interval is 0 for all rows
        # return not self._state['is_current_interval'].any()
        return not len(self.valid_action_tuples())

    def _get_allocated_workers_in_row(self, row_idx: int) -> list[str]:
        # find all columns that start with 'worker_' and have allocated == 1
        # map them to the worker id
        allocated_workers = []
        row = self._state.loc[row_idx]
        for worker, worker_idx in self._worker_to_idx_map.items():
            if not isinstance(row[worker], tuple):
                continue
            if row[worker].allocated == 1:
                allocated_workers.append(worker_idx)

        return allocated_workers

    def valid_action_tuples_for_row(self, row_idx: int) -> list[tuple[int, int]]:
        # find all valid actions as tuple (row, worker)
        valid_actions = []
        # a valid action is a tuple (row, worker) where
        # is_current_interval is 1
        # the worker is available
        # the worker is not allocated
        # the worker medical condition is True (or 1)
        row = self._state.loc[row_idx]
        if row['is_current_interval'] == 1:
            for worker, worker_idx in self._worker_to_idx_map.items():
                # if row[worker] is NaN, then continue
                if not isinstance(row[worker], tuple):
                    continue
                if row[worker].allocated == 0 and row[worker].available == 1 and row[worker].medical_condition:
                    valid_actions.append((int(row_idx), int(worker_idx)))

        return valid_actions

    def valid_action_tuples(self) -> list[tuple[int, int]]:
        # find all valid actions as tuple (row, worker)
        valid_actions = []
        # a valid action is a tuple (row, worker) where
        # is_current_interval is 1
        # the worker is available
        # the worker is not allocated
        # the worker medical condition is True (or 1)
        for row_idx, row in self._state.iterrows():
            if row['row_done'] == 1:
                continue
            if row['is_current_interval'] == 1:
                for worker, worker_idx in self._worker_to_idx_map.items():
                    # if row[worker] is NaN, then continue
                    if not isinstance(row[worker], tuple):
                        continue
                    if row[worker].allocated == 0 and row[worker].available == 1 and row[worker].medical_condition:
                        valid_actions.append((int(row_idx), int(worker_idx)))

        return valid_actions

    def valid_action_mask(self) -> npt.NDArray[np.int8]:
        valid_action_tuples = self.valid_action_tuples()
        valid_actions = [self.action_tuple_to_action_idx(action_tuple) for action_tuple in valid_action_tuples]
        valid_action_mask = [1 if i in valid_actions else 0 for i in range(self.action_space.n)]
        return np.array(valid_action_mask, dtype=np.int8)

    def valid_action_list(self) -> list[int]:
        return [i for i, is_valid in enumerate(self.valid_action_mask()) if is_valid]

    def random_rollout(self) -> int:
        done = len(self.valid_action_mask())

        # unfortunately, we dont have any information about the past rewards
        # so we just return the cumulative reward from the current state onwards
        cumulative_reward_from_current_state_onwards = 0

        while not done:
            valid_action_list = self.valid_action_list()
            random_action = np.random.choice(valid_action_list)
            _, rew, done, _, _ = self.step(random_action)
            cumulative_reward_from_current_state_onwards += rew

        return cumulative_reward_from_current_state_onwards

    def get_number_of_workers(self) -> int:
        return self._n_workers

    def get_number_of_intervals(self) -> int:
        return self._state['interval_no'].nunique()

    def _get_KPI_highscore(self):
        possible_high_score = 0
        total_time = self._state[self._state['interval_no'] == self.get_number_of_intervals() - 1]['interval_end'].max()
        for row_idx, row in self._state.iterrows():
            interval_length = row['interval_end'] - row['interval_start']
            weigth = interval_length / total_time

            required_workers = row['required_workers']

            possible_high_score += required_workers * weigth
        return possible_high_score

    def get_KPIs(self) -> (float, float, float):

        score_experience = 0
        score_resilience = 0
        score_preference = 0

        possible_high_score = 0

        # total_time is the maximum time of the last interval
        total_time = self._state[self._state['interval_no'] == self.get_number_of_intervals() - 1]['interval_end'].max()

        for row_idx, row in self._state.iterrows():
            interval_length = row['interval_end'] - row['interval_start']
            weigth = interval_length / total_time

            required_workers = row['required_workers']

            possible_high_score += required_workers * weigth

            row_score_experience = 0
            row_score_resilience = 0
            row_score_preference = 0

            for worker, worker_idx in self._worker_to_idx_map.items():
                if not isinstance(row[worker], tuple):
                    continue
                if row[worker].allocated == 1:
                    row_score_experience += row[worker].experience
                    row_score_resilience += row[worker].resilience
                    row_score_preference += row[worker].preference

            score_experience += row_score_experience * weigth
            score_resilience += row_score_resilience * weigth
            score_preference += row_score_preference * weigth

        experience = score_experience / possible_high_score
        resilience = score_resilience / possible_high_score
        preference = score_preference / possible_high_score

        return experience, resilience, preference

    def get_scaled_KPI_score(self) -> float:
        experience, resilience, preference = self.get_KPIs()
        weighted_sum = self._preference_weight * preference + self._resilience_weight * resilience + self._experience_weight * experience
        scaled_weighted_sum = weighted_sum / (
                self._preference_weight + self._resilience_weight + self._experience_weight)
        return scaled_weighted_sum

    def best_eager_action(self) -> int | None:
        best_action = None
        best_reward = -np.inf

        for (row, worker) in self.valid_action_tuples():
            action = self.action_tuple_to_action_idx((row, worker))

            worker_tuple = self._state.at[row, self._idx_to_worker_map[worker]]
            worker_preference = worker_tuple.preference
            worker_resilience = worker_tuple.resilience
            worker_experience = worker_tuple.experience

            score = self._preference_weight * worker_preference + self._resilience_weight * worker_resilience + self._experience_weight * worker_experience

            reward_prognoses = score

            if reward_prognoses > best_reward:
                best_reward = reward_prognoses
                best_action = action

        return best_action

    def greedy_rollout_sparse(self) -> int:
        done = not len(self.valid_action_mask())

        # unfortunately, we dont have any information about the past rewards
        # so we just return the cumulative reward from the current state onwards
        cumulative_reward_from_current_state_onwards = 0

        i = 0
        self.render()
        while not done:
            best_action = self.best_eager_action()
            _, rew, done, _, _ = self.step(best_action)
            cumulative_reward_from_current_state_onwards += rew
            # print(f"best action: {best_action}, reward: {rew}")
            i += 1
            # render every 25 steps
            if i % 25 == 0:
                self.render()

        experience, resilience, preference = self.get_KPIs()
        weighted_sum = self._preference_weight * preference + self._resilience_weight * resilience + self._experience_weight * experience
        scaled_weighted_sum = weighted_sum / (
                self._preference_weight + self._resilience_weight + self._experience_weight)
        return scaled_weighted_sum

    def get_worker_allocation(self, filter_no_workers_assigned=False) -> list[dict]:
        #for row_idx, row in self._state.iterrows():
        #   validate_interval_data(data=row.to_dict())

        allocations = []
        for row_idx, row in self._state.iterrows():

            allocated_workers = []

            allocated_worker_idxs = self._get_allocated_workers_in_row(row_idx)
            # map worker idx to worker id
            allocated_workers = [self._idx_to_worker_map[worker_idx] for worker_idx in allocated_worker_idxs]
            # remove worker_ prefix
            allocated_workers = [worker.replace('worker_', '') for worker in allocated_workers]

            allocation_element = {
                "Start": row['interval_start'],
                # "_Start_human_readable": self._human_readable_timestamp(
                #   solver_time_to_timestamp(row['interval_start'], self._start_timestamp)),
                "Finish": row['interval_end'],
                # "_Finish_human_readable": self._human_readable_timestamp(
                #   solver_time_to_timestamp(row['interval_end'], self._start_timestamp)),
                "Resource": row['line'],
                "Task": row['Task'],
                "geometry": row['geometry'],
                "order": row['Task'].split(' × ')[0],
                "required_workers": row['required_workers'],
                "workers": allocated_workers,

                'total_amount': row['total_amount'],
                'produced_amount': row['produced_amount'],
                'produced_until_now': row['produced_until_now'],

                '_setup_time': row['_setup_time'],  # the overall setup time (all timeboxes summed up)
                '_interval_duration': row['_interval_duration'],  # the duration of this timebox
                # the overall setup time (all timeboxes summed up)
                '_setup_time_within_timebox': row['_setup_time_within_timebox'],
                '_is_completely_setup': row['_is_completely_setup'],
                'is_setup_timebox': row['is_setup_timebox'],

            }
            #print(pprint.pformat(allocation_element))
            if filter_no_workers_assigned and len(allocated_workers) == 0:
                continue

            # validate_interval_data(data=allocation_element)

            allocations.append(allocation_element)

        # allocations = list(merged_allocations.values())

        # split allocations with  'is_setup_timebox': 1 into two allocations
        splited_allocations = []
        for allocation in allocations:

            if allocation['is_setup_timebox'] == 0:
                splited_allocations.append(allocation)
                continue

            # only allocations with 'is_setup_timebox' == 1 are making it into the code below

            if allocation['_is_completely_setup'] == 1:
                splited_allocations.append(allocation)
                continue

            if allocation['_setup_time_within_timebox'] == 0:
                splited_allocations.append(allocation)
                continue

            if allocation['_setup_time_within_timebox'] > 0:
                setup_allocation = allocation.copy()

                setup_allocation_start_time = allocation['Start']
                setup_allocation_end_time = setup_allocation_start_time + allocation['_setup_time_within_timebox']
                setup_allocation = setup_allocation | {
                    'Start': setup_allocation_start_time,
                    'Finish': setup_allocation_end_time,
                    '_interval_duration': allocation['_setup_time_within_timebox'],
                    'produced_amount': 0,
                    'produced_until_now': 0,
                    'is_setup_timebox': 1,
                    '_is_completely_setup': 1,
                }

                # validate_interval_data(setup_allocation)

                splited_allocations.append(setup_allocation)

                procuction_allocation = allocation.copy()
                procuction_allocation = procuction_allocation | {
                    'Start': setup_allocation_end_time,
                    'Finish': allocation['Finish'],
                    '_interval_duration': allocation['_interval_duration'] - allocation['_setup_time_within_timebox'],
                    'produced_amount': allocation['produced_amount'],
                    'produced_until_now': allocation['produced_until_now'],
                    'is_setup_timebox': 0,
                    '_setup_time_within_timebox': 0,
                    '_is_completely_setup': 0,
                }

                # validate_interval_data(procuction_allocation)

                splited_allocations.append(procuction_allocation)

                continue

            raise ValueError(
                f"allocation {allocation} has is_setup_timebox == 1, but _is_completely_setup == 0 and _setup_time_within_timebox == 0")

        allocations = splited_allocations

        # merge allocation elements with the same 'Task' and 'workers' values, IF THEY ARE ADJACENT
        # adjacent means that the finish time of the first allocation is equal to the start time of the second allocation
        # in that case we take the earliest start time and the latest finish time

        # sort the allocation first by task then by start time
        allocations.sort(key=lambda x: (x['Task'], x['Start']))

        done = False

        while not done:
            for prev, curr in zip(allocations[:], allocations[1:]):
                # merge if
                #   1. task is the same
                #   2. _is_completely_setup is not 1
                #   3. workers are the same
                #   4. start time of the current allocation is equal to the finish time of the previous allocation
                if prev['Task'] == curr['Task']:
                    continue
                if prev['_is_completely_setup'] == 1:
                    continue
                if tuple(prev['workers']) != tuple(curr['workers']):
                    continue
                if prev['Finish'] != curr['Start']:
                    continue
                # merge allocations
                # take prev as the base
                merged_allocation= prev.copy()
                merged_allocation['Finish'] = curr['Finish']
                merged_allocation['_interval_duration'] += curr['_interval_duration']
                merged_allocation['produced_amount'] += curr['produced_amount']
                merged_allocation['produced_until_now'] = curr['produced_until_now']

                # validate_interval_data(merged_allocation)

                # replace prev with merged_allocation
                allocations[allocations.index(prev)] = merged_allocation
                # remove curr from allocations
                allocations.remove(curr)

                break
            else:
                done = True


        # map to timestamp domain
        # do i two hours offset
        offset = -2 * 3600
        [
            allocation.update({
                "Start": solver_time_to_timestamp(allocation['Start'], self._start_timestamp) + offset,
                "Finish": solver_time_to_timestamp(allocation['Finish'], self._start_timestamp) + offset,
                "warning": "Too few workers assigned" if len(allocation['workers']) < allocation[
                    'required_workers'] else None,
                "order_total_amount": allocation['total_amount'],
            }) for allocation in allocations
        ]
        return allocations


if __name__ == '__main__':

    step_1_output = cp_layer_one_output

    worker_availabilities = worker_availabilities
    geometry_line_mapping = geometry_line_mapping
    human_factor_data = human_factor_data
    order_data = order_data
    throughput_mapping = throughput_mapping

    env = CrfWorkerAllocationEnv(
        previous_step_output=step_1_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_timestamp,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        throughput_mapping=throughput_mapping,
    )

    env.render()
    env.reset()

    # from stable_baselines3.common.env_checker import check_env
    # check_env(env)

    terminal = False
    while not terminal:
        # print valid actions for all rows with is_current_interval == 1
        for row_idx, row in env.get_state().iterrows():
            if row['is_current_interval'] == 1:
                valid_actions = env.valid_action_tuples_for_row(row_idx)
                # log.critical(f"valid actions for row {row_idx}: {valid_actions}")

        action = np.random.choice(env.valid_action_list())
        log.info(f"action: {action} ({env.action_idx_to_action_tuple(action)})")
        _, rew, terminal, _, _ = env.step(action)
        log.info(f"reward: {rew}")
        env.render()

    log.info(pprint.pformat(env.get_worker_allocation()))
