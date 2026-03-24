"""Test Airflow DAG structure and validity."""

import os
import sys
from pathlib import Path
import pytest

# Add airflow dags to path
dags_dir = Path(__file__).parent.parent / 'airflow' / 'dags'
sys.path.insert(0, str(dags_dir))


def test_dag_imports():
    """Test all DAGs can be imported."""
    import importlib.util

    dag_files = dags_dir.glob('*.py')
    for dag_file in dag_files:
        if dag_file.name.startswith('_'):
            continue

        spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
        module = importlib.util.module_from_spec(spec)

        # Should not raise
        spec.loader.exec_module(module)


def test_dag_structure():
    """Test DAG has proper structure."""
    from data_pipeline_dag import dag

    # DAG should have tasks
    assert len(dag.tasks) > 0
    print(f"✓ DAG has {len(dag.tasks)} tasks")

    # All tasks should have descriptions
    for task in dag.tasks:
        assert task.task_id, "Task missing task_id"
        print(f"  - {task.task_id}: OK")


def test_dag_dependencies():
    """Test DAG dependencies are valid."""
    from data_pipeline_dag import dag

    # Get task graph
    for task_id, task in dag.task_dict.items():
        # Each task should have upstream or downstream
        if task_id != 'setup_pipeline' and task_id != 'cleanup_pipeline':
            assert len(task.upstream_list) > 0 or len(task.downstream_list) > 0
            print(f"✓ {task_id} has dependencies")


def test_dag_schedule():
    """Test DAG scheduling is configured."""
    from data_pipeline_dag import dag

    assert dag.schedule_interval is not None
    print(f"✓ DAG schedule: {dag.schedule_interval}")

    assert dag.start_date is not None
    print(f"✓ DAG start date: {dag.start_date}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])