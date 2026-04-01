"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest

from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    customers = pd.DataFrame({
        "customer_id": [1],
        "name": ["Ahmad"],
        "city": ["Amman"],
        "registration_date": ["2024-01-01"],
    })

    products = pd.DataFrame({
        "product_id": [101],
        "name": ["Laptop"],
        "category": ["Electronics"],
        "unit_price": [1000.0],
    })

    orders = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [1, 1],
        "order_date": ["2024-01-10", "2024-01-11"],
        "status": ["completed", "cancelled"],
    })

    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [1, 2],
        "product_id": [101, 101],
        "quantity": [1, 1],
    })

    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    result = transform(data)

    assert len(result) == 1
    assert result.loc[0, "total_orders"] == 1
    assert result.loc[0, "total_revenue"] == 1000.0


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    customers = pd.DataFrame({
        "customer_id": [1],
        "name": ["Ahmad"],
        "city": ["Amman"],
        "registration_date": ["2024-01-01"],
    })

    products = pd.DataFrame({
        "product_id": [101],
        "name": ["Laptop"],
        "category": ["Electronics"],
        "unit_price": [1000.0],
    })

    orders = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [1, 1],
        "order_date": ["2024-01-10", "2024-01-11"],
        "status": ["completed", "completed"],
    })

    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [1, 2],
        "product_id": [101, 101],
        "quantity": [1, 150],
    })

    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    result = transform(data)

    assert len(result) == 1
    assert result.loc[0, "total_orders"] == 1
    assert result.loc[0, "total_revenue"] == 1000.0


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    df = pd.DataFrame({
        "customer_id": [None],
        "customer_name": ["Ahmad"],
        "city": ["Amman"],
        "total_orders": [1],
        "total_revenue": [1000.0],
        "avg_order_value": [1000.0],
        "top_category": ["Electronics"],
    })

    with pytest.raises(ValueError):
        validate(df)