# Copyright 2026 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Optional out-of-band payload transfer for SimpleStorage."""

from transfer_queue.storage.payload_transfer.base import (
    DEFAULT_INLINE_PAYLOAD_BYTES,
    PayloadDescriptor,
    PayloadTransfer,
    PayloadTransferError,
    ReceiveToken,
    TransferEndpoint,
)
from transfer_queue.storage.payload_transfer.factory import (
    create_payload_transfer,
    normalize_payload_transfer,
)

__all__ = [
    "DEFAULT_INLINE_PAYLOAD_BYTES",
    "PayloadDescriptor",
    "PayloadTransfer",
    "PayloadTransferError",
    "ReceiveToken",
    "TransferEndpoint",
    "create_payload_transfer",
    "normalize_payload_transfer",
]
