# process_state.py

# Global dictionary to track migration status
migration_status = {
    'running': False,
    'progress': 'Not started',
    'details': ''
}

# Global flag to signal cancellation
cancel_requested = False
