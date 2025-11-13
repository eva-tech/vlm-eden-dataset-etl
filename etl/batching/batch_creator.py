"""Batch creation module for splitting data into manageable batches."""

import logging
from typing import List, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BatchCreator:
    """Creates batches from data for parallel processing."""

    def __init__(self, batch_size: int):
        """Initialize batch creator.

        :param batch_size: Maximum size of each batch
        """
        self.batch_size = batch_size

    def create_batches(self, data: List[T]) -> List[List[T]]:
        """Split data into batches.

        :param data: List of items to batch
        :return: List of batches (each batch is a list of items)
        """
        batches = []
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batches.append(batch)
        logger.info(f"Created {len(batches)} batches from {len(data)} items (batch_size={self.batch_size})")
        return batches

    def get_batch_count(self, total_items: int) -> int:
        """Calculate number of batches needed for total items.

        :param total_items: Total number of items
        :return: Number of batches
        """
        return (total_items + self.batch_size - 1) // self.batch_size

