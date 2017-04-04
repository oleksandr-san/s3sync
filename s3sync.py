import argparse
import boto3
import os
import time
import itertools


class TreeNode(object):
    """Class for object tree node representation"""

    def __init__(self, parent, relative_path, is_directory, data):
        self.parent = parent
        self.children = []
        self.relative_path = relative_path
        self.is_directory = is_directory
        self.data = data

    def traverse(self, recursively=False):
        if recursively:
            for child in self.children:
                yield child
                yield from child.traverse(recursively)
        else:
            for child in self.children:
                yield child


class ObjectTree(object):
    """Class for object tree representation"""

    def __init__(self):
        self.root_node = None
        self.nodes_registry = {}

    def add_node(self, parent: object, relative_path: object, is_directory: object, data: object) -> object:
        node = TreeNode(parent, relative_path, is_directory, data)
        if parent:
            parent.children.append(node)
        self.nodes_registry[relative_path] = node
        return node

    def add_root_node(self, data):
        self.root_node = self.add_node(None, '', True, data)
        return self.root_node

    def get_node(self, relative_path):
        return self.nodes_registry.get(relative_path)


class ObjectSynchronizer(object):
    """Class that executes object synchronization routines"""

    def __init__(self):
        self.object_path = None
        self.root_path = None
        self.local_tree = None

        self.bucket = None
        self.bucket_tree = None

        self.synchronization_list = None

    def set_environment(self, object_path, root_path, bucket_name, credentials_path):
        if not root_path:
            self.object_path = os.path.abspath(object_path)
            if not os.path.exists(self.object_path):
                raise RuntimeError('Path error: \'{}\' does not exist'.format(self.object_path))
            self.root_path = self.object_path if os.path.isdir(self.object_path) \
                else os.path.dirname(self.object_path)
        else:
            self.root_path = os.path.abspath(root_path)
            if not os.path.exists(self.root_path):
                raise RuntimeError('Path error: \'{}\' does not exist'.format(self.root_path))
            self.object_path = os.path.abspath(object_path) if os.path.isabs(object_path) \
                else os.path.abspath(os.path.join(self.root_path, object_path))
            if os.path.commonpath([self.object_path, self.root_path]) != self.root_path:
                raise RuntimeError('Path error: Object path and root path are incompatible')

        credentials = self.extract_credentials(credentials_path)
        service = boto3.resource('s3', aws_access_key_id=credentials[0], aws_secret_access_key=credentials[1])
        self.bucket = service.Bucket(bucket_name)

    def extract_credentials(self, credentials_path):
        credentials_path = os.path.abspath(os.path.join(self.root_path, credentials_path)) \
                if not os.path.isabs(credentials_path) else \
                os.path.abspath(credentials_path)
        if not os.path.exists(credentials_path):
            raise RuntimeError('Credentials path {} is not valid'.format(credentials_path))

        with open(credentials_path, 'r+') as f:
            credentials = tuple(i.strip() for i in f.readlines()[1].split(','))
        return credentials

    @staticmethod
    def extract_local_node_data(path):
        data = {
            'mtime': os.path.getmtime(path),
            'size': os.path.getsize(path)
        }
        return data

    def extract_relative_path(self, full_path):
        if len(self.root_path) > len(full_path):
            raise RuntimeError('Invalid relative path conversion argument: {}'.format(full_path))
        relative_path = full_path[len(self.root_path):].lstrip('/\\')

        relative_path = relative_path.replace('\\', '/')
        if relative_path and os.path.isdir(full_path):
            relative_path += '/'

        return relative_path

    def build_local_tree(self):
        self.local_tree = ObjectTree()
        root_node = self.local_tree.add_root_node(self.extract_local_node_data(self.root_path))

        if self.root_path != self.object_path:
            parent_node = root_node
            object_relative_path = self.extract_relative_path(self.object_path)

            parent_paths = []
            parent_path = os.path.split(object_relative_path)[0]
            if object_relative_path.endswith('/'):
                parent_path = os.path.split(parent_path)[0]
            while parent_path:
                parent_paths.append(parent_path)
                parent_path = os.path.split(parent_path)[0]

            for parent_path in parent_paths[::-1]:
                parent_full_path = os.path.join(self.root_path, parent_path)
                if not os.path.exists(parent_full_path):
                    break
                parent_relative_path = self.extract_relative_path(parent_full_path)
                parent_data = self.extract_local_node_data(parent_full_path)
                parent_node = self.local_tree.add_node(parent_node, parent_relative_path, True, parent_data)

            if os.path.exists(self.object_path):
                object_data = self.extract_local_node_data(self.object_path)
                self.local_tree.add_node(parent_node, object_relative_path, False, object_data)

        if os.path.exists(self.object_path):
            for parent_full_path, dirs, files in os.walk(self.object_path):
                parent_relative_path = self.extract_relative_path(parent_full_path)
                parent_node = self.local_tree.get_node(parent_relative_path)
                if not parent_node:
                    break

                for obj_name in itertools.chain(dirs, files):
                    obj_full_path = os.path.join(parent_full_path, obj_name)
                    obj_relative_path = self.extract_relative_path(obj_full_path)
                    is_directory = obj_relative_path.endswith('/')
                    obj_data = self.extract_local_node_data(obj_full_path)
                    self.local_tree.add_node(parent_node, obj_relative_path, is_directory, obj_data)

    @staticmethod
    def extract_bucket_node_data(object_summary):
        data = {
            'mtime': time.mktime(object_summary.last_modified.timetuple()),
            'size': object_summary.size
        }
        return data

    def build_bucket_tree(self):
        self.bucket_tree = ObjectTree()
        self.bucket_tree.add_root_node(data={})

        for bucket_object in self.bucket.objects.all():
            object_relative_path = bucket_object.key
            parent_relative_path = ''

            path_components = object_relative_path.rsplit('/', 1)
            if len(path_components) == 2:
                if not path_components[1]:
                    path_components = path_components[0].rsplit('/', 1)
                    if len(path_components) == 2 and path_components[1]:
                        parent_relative_path = path_components[0] + '/'
                else:
                    parent_relative_path = path_components[0] + '/'

            parent_node = self.bucket_tree.get_node(parent_relative_path)
            if not parent_node:
                break

            is_directory = object_relative_path.endswith('/')
            object_data = self.extract_bucket_node_data(bucket_object)
            self.bucket_tree.add_node(parent_node, object_relative_path, is_directory, object_data)

    @staticmethod
    def process_trees_difference(object_path, source, target, found_node_handler, absent_node_handler):
        def process_node(node):
            target_node = target.get_node(node.relative_path)
            if target_node:
                found_node_handler(target_node, node)
            else:
                absent_node_handler(node.relative_path)

        source_node = source.get_node(object_path)
        if source_node:
            if source_node.relative_path:
                process_node(source_node)
            for child in source_node.traverse(recursively=True):
                process_node(child)

    @staticmethod
    def is_node_modified(target_node, source_node):
        target_data = target_node.data
        source_data = source_node.data

        if not target_data or not source_data:
            return True
        elif target_data['size'] != source_data['size']:
            return True
        # elif source_data['mtime'] - target_data['mtime'] > 60:
        #    return True
        else:
            return False

    def build_synchronization_list(self, synchronization_type):
        self.build_local_tree()
        self.build_bucket_tree()

        self.synchronization_list = {
            'LA': [], 'LU': [], 'LD': [],
            'BA': [], 'BU': [], 'BD': []
        }

        def found_local_node_handler(target_node, source_node):
            if synchronization_type == 1:
                return
            if self.is_node_modified(target_node, source_node):
                self.synchronization_list['LU'].append(source_node.relative_path)

        def absent_local_node_handler(node_path):
            if synchronization_type == 1:
                self.synchronization_list['BD'].append(node_path)
            else:
                self.synchronization_list['LA'].append(node_path)

        def found_bucket_node_handler(target_node, source_node):
            if synchronization_type == 2:
                return
            if self.is_node_modified(target_node, source_node):
                self.synchronization_list['BU'].append(source_node.relative_path)

        def absent_bucket_node_handler(node_path):
            if synchronization_type == 2:
                self.synchronization_list['LD'].append(node_path)
            else:
                self.synchronization_list['BA'].append(node_path)

        object_relative_path = self.extract_relative_path(self.object_path)

        self.process_trees_difference(
            object_relative_path, self.bucket_tree, self.local_tree,
            found_local_node_handler, absent_local_node_handler)

        self.process_trees_difference(
            object_relative_path, self.local_tree, self.bucket_tree,
            found_bucket_node_handler, absent_bucket_node_handler)

    def upload_bucket_object(self, key, full_path):
        print('Uploading \'{}\' object as \'{}\'... '.format(full_path, key), end='')
        if key.endswith('/'):
            self.bucket.put_object(Key=key)
        else:
            with open(full_path, 'rb') as f:
                self.bucket.Object(key).upload_fileobj(f)
        print('Done')

    def download_bucket_object(self, key, full_path):
        print('Downloading \'{}\' object as \'{}\'... '.format(key, full_path), end='')
        try:
            if key.endswith('/'):
                if not os.path.exists(full_path):
                    os.mkdir(full_path)
            else:
                with open(full_path, 'wb') as f:
                    self.bucket.Object(key).download_fileobj(f)
        except PermissionError as ex:
            print('Error: {}'.format(ex.strerror))
        else:
            print('Done')

    def delete_local_object(self, full_path):
        print('Deleting \'{}\' object... '.format(full_path), end='')
        try:
            if os.path.exists(full_path):
                os.remove(full_path)
        except PermissionError as ex:
            print('Error: {}'.format(ex.strerror))
        else:
            print('Done')

    def delete_bucket_object(self, key):
        print('Deleting \'{}\' object... '.format(key), end='')
        self.bucket.Object(Key=key).delete()
        print('Done')

    def execute_synchronization(self, execute_removal=False):
        for rel_path in itertools.chain(self.synchronization_list['BA'], self.synchronization_list['BU']):
            full_path = os.path.abspath(os.path.join(self.root_path, rel_path))
            self.upload_bucket_object(rel_path, full_path)

        for rel_path in itertools.chain(self.synchronization_list['LA'], self.synchronization_list['LU']):
            full_path = os.path.abspath(os.path.join(self.root_path, rel_path))
            self.download_bucket_object(rel_path, full_path)

        if execute_removal:
            for rel_path in self.synchronization_list['LD']:
                full_path = os.path.abspath(os.path.join(self.root_path, rel_path))
                self.delete_local_object(full_path)

            for rel_path in self.synchronization_list['BD']:
                self.delete_bucket_object(rel_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bucket objects synchronization script')

    parser.add_argument(
        'bucket_name',
        help='Bucket name')
    parser.add_argument(
        'object_path',
        help='Local storage synchronization object path')
    parser.add_argument(
        '-r', '--root_path',
        help='Local storage root path that corresponds to bucket root. By default object directory is used')
    parser.add_argument(
        '-c', '--credentials_path', default='accessKeys.csv',
        help='Path to .csv file with credentials')
    parser.add_argument(
        '-t', '--type', choices=[0, 1, 2], type=int, default=0,
        help='Synchronization type: 0 - bidirectional (two-way), 1 - local storage replication, 2 - bucket replication')
    parser.add_argument(
        '-d', '--delete', action="store_true",
        help='Delete absent files from target when using one-way synchronization')

    args = parser.parse_args()

    manager = ObjectSynchronizer()

    manager.set_environment(
        args.object_path, args.root_path,
        args.bucket_name, args.credentials_path)

    manager.build_synchronization_list(args.type)
    manager.execute_synchronization()

