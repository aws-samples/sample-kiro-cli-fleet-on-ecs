"""The worker: what runs inside one task.

Claims a subscription, drains work items, runs one headless Kiro CLI workflow per
item, verifies the result, and hands the capacity back.
"""
