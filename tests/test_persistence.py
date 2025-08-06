"""
Tests for pipeline and node persistence layer.
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from datetime import datetime

from remotemedia.persistence import (
    DatabaseManager,
    PipelineStore,
    NodeStore,
    StoredPipeline,
    StoredNode,
    AccessLevel
)
from remotemedia.persistence.migrations import MigrationManager
from remotemedia.core.pipeline_registry import PipelineRegistry


@pytest.fixture
async def db_manager():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = DatabaseManager(db_path)
    await db.initialize()
    
    # Apply migrations
    migrator = MigrationManager(db)
    await migrator.apply_all_migrations()
    
    yield db
    
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
async def pipeline_store(db_manager):
    """Create pipeline store."""
    return PipelineStore(db_manager)


@pytest.fixture
async def node_store(db_manager):
    """Create node store."""
    return NodeStore(db_manager)


@pytest.fixture
async def test_user(db_manager):
    """Create a test user."""
    user_id = "test_user_123"
    await db_manager.create_user(user_id, "testuser", "test@example.com")
    return user_id


class TestNodePersistence:
    """Test node persistence operations."""
    
    @pytest.mark.asyncio
    async def test_create_node(self, node_store, test_user):
        """Test creating and storing a node."""
        node = await node_store.create_node(
            name="Test Calculator",
            node_type="CalculatorNode",
            config={"operation": "add", "default_b": 10},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            description="A test calculator node",
            tags=["math", "calculator", "test"]
        )
        
        assert node.id is not None
        assert node.name == "Test Calculator"
        assert node.node_type == "CalculatorNode"
        assert node.config["operation"] == "add"
        assert node.owner_id == test_user
        assert node.access_level == AccessLevel.PUBLIC
        assert "math" in node.tags
    
    @pytest.mark.asyncio
    async def test_get_node(self, node_store, test_user):
        """Test retrieving a stored node."""
        # Create a node
        created = await node_store.create_node(
            name="Test Node",
            node_type="TestType",
            config={"key": "value"},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC
        )
        
        # Retrieve it
        retrieved = await node_store.get_node(created.id, test_user)
        
        assert retrieved is not None
        assert retrieved.id == created.id
        assert retrieved.name == created.name
        assert retrieved.config == created.config
    
    @pytest.mark.asyncio
    async def test_update_node(self, node_store, test_user):
        """Test updating a node."""
        # Create a node
        node = await node_store.create_node(
            name="Original Name",
            node_type="TestType",
            config={"key": "value"},
            owner_id=test_user
        )
        
        # Update it
        updated = await node_store.update_node(
            node.id,
            test_user,
            name="Updated Name",
            config={"key": "new_value", "extra": "data"},
            tags=["updated", "test"]
        )
        
        assert updated.name == "Updated Name"
        assert updated.config["key"] == "new_value"
        assert updated.config["extra"] == "data"
        assert "updated" in updated.tags
        assert updated.version == 2
    
    @pytest.mark.asyncio
    async def test_delete_node(self, node_store, test_user):
        """Test deleting a node."""
        # Create a node
        node = await node_store.create_node(
            name="To Delete",
            node_type="TestType",
            config={},
            owner_id=test_user
        )
        
        # Delete it
        deleted = await node_store.delete_node(node.id, test_user)
        assert deleted is True
        
        # Try to retrieve it
        retrieved = await node_store.get_node(node.id, test_user)
        assert retrieved is None
    
    @pytest.mark.asyncio
    async def test_list_nodes(self, node_store, test_user):
        """Test listing nodes with filters."""
        # Create multiple nodes
        await node_store.create_node(
            name="Node 1",
            node_type="TypeA",
            config={},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            tags=["tag1"]
        )
        
        await node_store.create_node(
            name="Node 2",
            node_type="TypeB",
            config={},
            owner_id=test_user,
            access_level=AccessLevel.PRIVATE,
            tags=["tag2"]
        )
        
        await node_store.create_node(
            name="Node 3",
            node_type="TypeA",
            config={},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            tags=["tag1", "tag2"],
            is_template=True
        )
        
        # List all nodes
        all_nodes = await node_store.list_nodes(user_id=test_user)
        assert len(all_nodes) == 3
        
        # Filter by type
        type_a_nodes = await node_store.list_nodes(
            user_id=test_user,
            node_type="TypeA"
        )
        assert len(type_a_nodes) == 2
        
        # Filter by access level
        public_nodes = await node_store.list_nodes(
            user_id=test_user,
            access_level=AccessLevel.PUBLIC
        )
        assert len(public_nodes) == 2
        
        # Filter by template
        templates = await node_store.list_nodes(
            user_id=test_user,
            is_template=True
        )
        assert len(templates) == 1
        
        # Filter by tags
        tag1_nodes = await node_store.list_nodes(
            user_id=test_user,
            tags=["tag1"]
        )
        assert len(tag1_nodes) == 2
    
    @pytest.mark.asyncio
    async def test_clone_node(self, node_store, test_user):
        """Test cloning a node."""
        # Create original
        original = await node_store.create_node(
            name="Original",
            node_type="TestType",
            config={"key": "value"},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            tags=["original"]
        )
        
        # Clone it
        clone = await node_store.clone_node(
            original.id,
            test_user,
            new_name="Cloned Node"
        )
        
        assert clone is not None
        assert clone.id != original.id
        assert clone.name == "Cloned Node"
        assert clone.config == original.config
        assert clone.owner_id == test_user
        assert clone.access_level == AccessLevel.PRIVATE  # Clones start private
        assert clone.metadata.get("cloned_from") == original.id


class TestPipelinePersistence:
    """Test pipeline persistence operations."""
    
    @pytest.mark.asyncio
    async def test_create_pipeline(self, pipeline_store, test_user):
        """Test creating and storing a pipeline."""
        definition = {
            "name": "test_pipeline",
            "nodes": [
                {"type": "Node1", "config": {}},
                {"type": "Node2", "config": {}}
            ],
            "connections": [{"from": 0, "to": 1}]
        }
        
        pipeline = await pipeline_store.create_pipeline(
            name="Test Pipeline",
            definition=definition,
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            description="A test pipeline",
            tags=["test", "example"]
        )
        
        assert pipeline.id is not None
        assert pipeline.name == "Test Pipeline"
        assert pipeline.definition == definition
        assert pipeline.owner_id == test_user
        assert "test" in pipeline.tags
    
    @pytest.mark.asyncio
    async def test_get_pipeline(self, pipeline_store, test_user):
        """Test retrieving a stored pipeline."""
        # Create a pipeline
        created = await pipeline_store.create_pipeline(
            name="Test Pipeline",
            definition={"nodes": []},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC
        )
        
        # Retrieve it
        retrieved = await pipeline_store.get_pipeline(created.id, test_user)
        
        assert retrieved is not None
        assert retrieved.id == created.id
        assert retrieved.name == created.name
        assert retrieved.definition == created.definition
    
    @pytest.mark.asyncio
    async def test_update_pipeline(self, pipeline_store, test_user):
        """Test updating a pipeline."""
        # Create a pipeline
        pipeline = await pipeline_store.create_pipeline(
            name="Original",
            definition={"nodes": []},
            owner_id=test_user
        )
        
        # Update it
        new_definition = {"nodes": [{"type": "NewNode"}]}
        updated = await pipeline_store.update_pipeline(
            pipeline.id,
            test_user,
            name="Updated",
            definition=new_definition,
            tags=["updated"]
        )
        
        assert updated.name == "Updated"
        assert updated.definition == new_definition
        assert "updated" in updated.tags
        assert updated.version == 2
    
    @pytest.mark.asyncio
    async def test_delete_pipeline(self, pipeline_store, test_user):
        """Test deleting a pipeline."""
        # Create a pipeline
        pipeline = await pipeline_store.create_pipeline(
            name="To Delete",
            definition={"nodes": []},
            owner_id=test_user
        )
        
        # Delete it
        deleted = await pipeline_store.delete_pipeline(pipeline.id, test_user)
        assert deleted is True
        
        # Try to retrieve it
        retrieved = await pipeline_store.get_pipeline(pipeline.id, test_user)
        assert retrieved is None
    
    @pytest.mark.asyncio
    async def test_clone_pipeline(self, pipeline_store, test_user):
        """Test cloning a pipeline."""
        # Create original
        original = await pipeline_store.create_pipeline(
            name="Original Pipeline",
            definition={"nodes": [{"type": "Node1"}]},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            tags=["original"]
        )
        
        # Clone it
        clone = await pipeline_store.clone_pipeline(
            original.id,
            test_user,
            new_name="Cloned Pipeline"
        )
        
        assert clone is not None
        assert clone.id != original.id
        assert clone.name == "Cloned Pipeline"
        assert clone.definition == original.definition
        assert clone.access_level == AccessLevel.PRIVATE
        assert clone.metadata.get("cloned_from") == original.id
    
    @pytest.mark.asyncio
    async def test_get_templates(self, pipeline_store, test_user):
        """Test retrieving pipeline templates."""
        # Create regular pipeline
        await pipeline_store.create_pipeline(
            name="Regular",
            definition={"nodes": []},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            is_template=False
        )
        
        # Create template
        await pipeline_store.create_pipeline(
            name="Template",
            definition={"nodes": []},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC,
            is_template=True
        )
        
        # Get templates
        templates = await pipeline_store.get_templates(test_user)
        
        assert len(templates) == 1
        assert templates[0].name == "Template"
        assert templates[0].is_template is True


class TestPipelineRegistry:
    """Test PipelineRegistry with persistence."""
    
    @pytest.mark.asyncio
    async def test_registry_with_persistence(self):
        """Test registry with persistence enabled."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            # Create registry with persistence
            registry = PipelineRegistry(db_path=db_path, enable_persistence=True)
            await registry.initialize()
            
            # Register a pipeline with persistence
            definition = {
                "name": "test_pipeline",
                "nodes": [{"type": "Node1"}],
                "connections": []
            }
            
            pipeline_id = await registry.register_pipeline(
                name="Test Pipeline",
                definition=definition,
                metadata={"tags": ["test"]},
                owner_id="test_user",
                access_level=AccessLevel.PUBLIC,
                persist=True
            )
            
            assert pipeline_id is not None
            
            # Save an in-memory pipeline
            saved = await registry.save_pipeline(
                pipeline_id,
                owner_id="test_user",
                access_level=AccessLevel.PUBLIC
            )
            assert saved is True
            
            # Load a pipeline from storage
            loaded_id = await registry.load_pipeline(
                pipeline_id,
                user_id="test_user"
            )
            assert loaded_id == pipeline_id
            
        finally:
            Path(db_path).unlink(missing_ok=True)
    
    @pytest.mark.asyncio
    async def test_registry_without_persistence(self):
        """Test registry without persistence."""
        registry = PipelineRegistry(enable_persistence=False)
        
        # Register a pipeline without persistence
        definition = {
            "name": "test_pipeline",
            "nodes": [{"type": "Node1"}],
            "connections": []
        }
        
        pipeline_id = await registry.register_pipeline(
            name="Test Pipeline",
            definition=definition
        )
        
        assert pipeline_id is not None
        assert pipeline_id in registry.pipelines
        
        # Try to save (should fail)
        saved = await registry.save_pipeline(
            pipeline_id,
            owner_id="test_user"
        )
        assert saved is False


class TestAccessControl:
    """Test access control for pipelines and nodes."""
    
    @pytest.mark.asyncio
    async def test_private_access(self, pipeline_store, test_user):
        """Test private access control."""
        other_user = "other_user_456"
        await pipeline_store.db.create_user(other_user, "otheruser")
        
        # Create private pipeline
        pipeline = await pipeline_store.create_pipeline(
            name="Private Pipeline",
            definition={"nodes": []},
            owner_id=test_user,
            access_level=AccessLevel.PRIVATE
        )
        
        # Owner can access
        retrieved = await pipeline_store.get_pipeline(pipeline.id, test_user)
        assert retrieved is not None
        
        # Other user cannot access
        retrieved = await pipeline_store.get_pipeline(pipeline.id, other_user)
        assert retrieved is None
        
        # Anonymous cannot access
        retrieved = await pipeline_store.get_pipeline(pipeline.id, None)
        assert retrieved is None
    
    @pytest.mark.asyncio
    async def test_public_access(self, pipeline_store, test_user):
        """Test public access control."""
        other_user = "other_user_789"
        await pipeline_store.db.create_user(other_user, "otheruser")
        
        # Create public pipeline
        pipeline = await pipeline_store.create_pipeline(
            name="Public Pipeline",
            definition={"nodes": []},
            owner_id=test_user,
            access_level=AccessLevel.PUBLIC
        )
        
        # Everyone can access
        retrieved = await pipeline_store.get_pipeline(pipeline.id, test_user)
        assert retrieved is not None
        
        retrieved = await pipeline_store.get_pipeline(pipeline.id, other_user)
        assert retrieved is not None
        
        retrieved = await pipeline_store.get_pipeline(pipeline.id, None)
        assert retrieved is not None
    
    @pytest.mark.asyncio
    async def test_readonly_access(self, pipeline_store, test_user):
        """Test readonly access control."""
        other_user = "other_user_readonly"
        await pipeline_store.db.create_user(other_user, "otheruser")
        
        # Create readonly pipeline
        pipeline = await pipeline_store.create_pipeline(
            name="Readonly Pipeline",
            definition={"nodes": []},
            owner_id=test_user,
            access_level=AccessLevel.READONLY
        )
        
        # Everyone can read
        retrieved = await pipeline_store.get_pipeline(pipeline.id, other_user)
        assert retrieved is not None
        
        # Only owner can update
        updated = await pipeline_store.update_pipeline(
            pipeline.id,
            other_user,  # Not the owner
            name="Try to Update"
        )
        assert updated is None  # Update denied
        
        updated = await pipeline_store.update_pipeline(
            pipeline.id,
            test_user,  # Owner
            name="Owner Update"
        )
        assert updated is not None
        assert updated.name == "Owner Update"


class TestMigrations:
    """Test database migrations."""
    
    @pytest.mark.asyncio
    async def test_migration_system(self, db_manager):
        """Test migration system."""
        migrator = MigrationManager(db_manager)
        
        # Get initial version
        version = await migrator.get_current_version()
        assert version >= 0
        
        # Apply all migrations
        await migrator.apply_all_migrations()
        
        # Check version increased
        new_version = await migrator.get_current_version()
        assert new_version >= version
        
        # Re-applying should be safe
        await migrator.apply_all_migrations()
        assert await migrator.get_current_version() == new_version


if __name__ == "__main__":
    pytest.main([__file__, "-v"])