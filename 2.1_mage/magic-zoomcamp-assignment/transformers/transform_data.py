import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
    """
    Convert CamelCase to snake_case and return the name along with a flag indicating a change.
    """
    original_name = name
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    # Check if the name was changed
    name_changed = (original_name != name)
    return name, name_changed

@transformer
def transform(data):
    # Initialize a counter for the number of names changed
    total_changes = 0
    
    # Remove rows where passenger count is 0 or trip distance is 0
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    
    # Create a new column 'lpep_pickup_date' from 'lpep_pickup_datetime'
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    # Rename columns from Camel Case to Snake Case and count changes
    new_columns = []
    for column in data.columns:
        new_name, changed = camel_to_snake(column)
        new_columns.append(new_name)
        if changed:
            total_changes += 1
    data.columns = new_columns

    print(data['vendor_id'].unique())
    print(total_changes)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['passenger_count'] > 0).all(), "There are rides with zero passengers"
    assert (output['trip_distance'] > 0).all(), "There are rides with zero distance"