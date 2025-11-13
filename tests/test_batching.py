"""Unit tests for batching logic."""

import pytest

from etl.batching.batch_creator import BatchCreator

pytestmark = pytest.mark.unit


class TestBatchCreator:
    """Test cases for BatchCreator."""

    def test_create_batches_empty_list(self):
        """Test creating batches from empty list."""
        creator = BatchCreator(batch_size=10)
        batches = creator.create_batches([])
        assert batches == []
        assert len(batches) == 0

    def test_create_batches_single_batch(self):
        """Test creating batches when all items fit in one batch."""
        creator = BatchCreator(batch_size=10)
        data = list(range(5))
        batches = creator.create_batches(data)
        assert len(batches) == 1
        assert batches[0] == [0, 1, 2, 3, 4]

    def test_create_batches_multiple_batches(self):
        """Test creating multiple batches."""
        creator = BatchCreator(batch_size=3)
        data = list(range(10))
        batches = creator.create_batches(data)
        assert len(batches) == 4
        assert batches[0] == [0, 1, 2]
        assert batches[1] == [3, 4, 5]
        assert batches[2] == [6, 7, 8]
        assert batches[3] == [9]

    def test_create_batches_exact_multiple(self):
        """Test creating batches when data size is exact multiple of batch size."""
        creator = BatchCreator(batch_size=5)
        data = list(range(20))
        batches = creator.create_batches(data)
        assert len(batches) == 4
        for batch in batches:
            assert len(batch) == 5

    def test_create_batches_larger_than_data(self):
        """Test creating batches when batch size is larger than data."""
        creator = BatchCreator(batch_size=100)
        data = list(range(10))
        batches = creator.create_batches(data)
        assert len(batches) == 1
        assert batches[0] == list(range(10))

    def test_get_batch_count(self):
        """Test calculating batch count."""
        creator = BatchCreator(batch_size=10)
        assert creator.get_batch_count(0) == 0
        assert creator.get_batch_count(10) == 1
        assert creator.get_batch_count(11) == 2
        assert creator.get_batch_count(20) == 2
        assert creator.get_batch_count(21) == 3

    def test_create_batches_with_strings(self):
        """Test creating batches with string data."""
        creator = BatchCreator(batch_size=2)
        data = ['a', 'b', 'c', 'd', 'e']
        batches = creator.create_batches(data)
        assert len(batches) == 3
        assert batches[0] == ['a', 'b']
        assert batches[1] == ['c', 'd']
        assert batches[2] == ['e']

    def test_create_batches_with_dicts(self):
        """Test creating batches with dictionary data."""
        creator = BatchCreator(batch_size=2)
        data = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'},
            {'id': 3, 'name': 'C'},
        ]
        batches = creator.create_batches(data)
        assert len(batches) == 2
        assert batches[0] == [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
        assert batches[1] == [{'id': 3, 'name': 'C'}]

