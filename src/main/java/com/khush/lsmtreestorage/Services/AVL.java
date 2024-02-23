package com.khush.lsmtreestorage.Services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AVL {
    private Node root;

    public void insert(String key, String value) {
        root = insert(root, key, value);
    }

    public void update(String key, String value) {
        root = update(root, key, value);
    }

    public void delete(String key) {
        root = deleteNode(root, key);
    }

    public List<List<String>> getInOrderTraversal() {
        List<List<String>> res = new ArrayList<>();
        getInOrderTraversal(root, res);
        return res;
    }

    public boolean findKey(String key) {
        return findKey(root, key);
    }

    public String findValue(String key) {
        return findValue(root, key);
    }

    public void empty() {
        root = null;
    }

    public void display() {
        display(root);
    }

    public void printTree() {
        printTreeHelper(root, "", true);
    }

    private Node insert(Node node, String key, String value) {
        if (node == null) {
            return (new Node(key, value));
        }

        if (key.compareTo(node.key) < 0) {
            node.left = insert(node.left, key, value);
        } else if (key.compareTo(node.key) > 0) {
            node.right = insert(node.right, key, value);
        } else {
            return node;
        }

        node.height = 1 + max(height(node.left), height(node.right));

        int balance = getBalance(node);

        if (balance > 1 && key.compareTo(node.left.key) < 0) {
            return rightRotate(node);
        }

        if (balance < -1 && key.compareTo(node.right.key) > 0) {
            return leftRotate(node);
        }

        if (balance > 1 && key.compareTo(node.left.key) > 0) {
            node.left = leftRotate(node.left);
            return rightRotate(node);
        }

        if (balance < -1 && key.compareTo(node.right.key) < 0) {
            node.right = rightRotate(node.right);
            return leftRotate(node);
        }

        return node;
    }
    
    private Node update(Node node, String key, String value) {
        // Key not found in tree
        if (node == null) {
            return null;
        }
    
        // Traverse to the right place
        int cmp = key.compareTo(node.key);
        if (cmp < 0) {
            node.left = update(node.left, key, value);
        } else if (cmp > 0) {
            node.right = update(node.right, key, value);
        } else {
            // Update the value of the node
            node.value = value;
        }
    
        // Update height and balance tree
        node.height = 1 + Math.max(height(node.left), height(node.right));
        int balance = getBalance(node);
    
        // Left Left Case
        if (balance > 1 && key.compareTo(node.left.key) < 0) {
            return rightRotate(node);
        }
    
        // Right Right Case
        if (balance < -1 && key.compareTo(node.right.key) > 0) {
            return leftRotate(node);
        }
    
        // Left Right Case
        if (balance > 1 && key.compareTo(node.left.key) > 0) {
            node.left = leftRotate(node.left);
            return rightRotate(node);
        }
    
        // Right Left Case
        if (balance < -1 && key.compareTo(node.right.key) < 0) {
            node.right = rightRotate(node.right);
            return leftRotate(node);
        }
    
        return node;
    }

    private Node deleteNode(Node root, String key) {
        if (root == null) {
            return root;
        }
    
        if (key.compareTo(root.key) < 0) {
            root.left = deleteNode(root.left, key);
        }
        else if (key.compareTo(root.key) > 0) {   
            root.right = deleteNode(root.right, key);
        } else {
            if ((root.left == null) || (root.right == null)) {
                Node temp = null;
                
                if (temp == root.left) {
                    temp = root.right;
                } else {
                    temp = root.left;
                }

                if (temp == null) {
                    temp = root;
                    root = null;
                } else {
                    root = temp;
                }

            } else {
                Node temp = minValueNode(root.right);
                root.key = temp.key;
                root.value = temp.value; // Update the value as well
                root.right = deleteNode(root.right, temp.key);
            }
        }
    
        if (root == null) {
            return root;
        }
    
        root.height = max(height(root.left), height(root.right)) + 1;
    
        int balance = getBalance(root);
    
        if (balance > 1 && getBalance(root.left) >= 0) {
            return rightRotate(root);
        }
    
        if (balance > 1 && getBalance(root.left) < 0) {
            root.left = leftRotate(root.left);
            return rightRotate(root);
        }
    
        if (balance < -1 && getBalance(root.right) <= 0) {
            return leftRotate(root);
        }
    
        if (balance < -1 && getBalance(root.right) > 0) {
            root.right = rightRotate(root.right);
            return leftRotate(root);
        }
    
        return root;
    }

    private Node rightRotate(Node y) {
        Node x = y.left;
        Node t2 = x.right;
        x.right = y;
        y.left = t2;
        y.height = max(height(y.left), height(y.right)) + 1;
        x.height = max(height(x.left), height(x.right)) + 1;
        return x;
    }

    private Node leftRotate(Node x) {
        Node y = x.right;
        Node t2 = y.left;
        y.left = x;
        x.right = t2;
        x.height = max(height(x.left), height(x.right)) + 1;
        y.height = max(height(y.left), height(y.right)) + 1;
        return y;
    }

    private int height(Node node) {
        if(node == null) {
            return 0;
        }
        return node.height;
    }

    private int max(int a, int b) {
        return (a > b) ? a : b;
    }

    private int getBalance(Node node) {
        if(node == null) {
            return 0;
        }
        return height(node.left) - height(node.right);
    }

    private Node minValueNode(Node node) {
        Node current = node;
        while (current.left != null) {
            current = current.left;
        }
        return current;
    }

    private void display(Node node) {
        if (node != null) {
            String leftKey = node.left != null ? node.left.key : "null";
            String rightKey = node.right != null ? node.right.key : "null";
            System.out.println("Node Key: " + node.key + ", Value: " + node.value + ", Left Node: " + leftKey + ", Right Node: " + rightKey);
            display(node.left);
            display(node.right);
        }
    }

    private void printTreeHelper(Node node, String indent, boolean isLast) {
        System.out.print(indent);
        if (isLast) {
            System.out.print("└─ ");
            indent += "  ";
        } else {
            System.out.print("├─ ");
            indent += "| ";
        }
    
        if (node != null) {
            System.out.println(node.key + " : " + node.value);
            if (node.left != null) {
                printTreeHelper(node.left, indent, false);
            } else {
                printNullNode(indent, false);
            }
            
            if (node.right != null) {
                printTreeHelper(node.right, indent, true);
            } else {
                printNullNode(indent, true);
            }
        } else {
            System.out.println("null");
        }
    }

    private void printNullNode(String indent, boolean isLast) {
        System.out.print(indent);
        if (isLast) {
            System.out.print("└─ ");
        } else {
            System.out.print("├─ ");
        }
        System.out.println("null");
    }

    private boolean findKey(Node node, String key) {
        if(node == null) {
            return false;
        }

        int cmp = key.compareTo(node.key);
        if(cmp < 0) {
            return findKey(node.left, key);
        } else if(cmp > 0) {
            return findKey(node.right, key);
        } else {
            return true;
        }
    }

    private String findValue(Node node, String key) {
        if(node == null) {
            return null;
        }

        int cmp = key.compareTo(node.key);
        if(cmp < 0) {
            return findValue(node.left, key);
        } else if(cmp > 0) {
            return findValue(node.right, key);
        } else {
            return node.value;
        }
    }

    private List<List<String>> getInOrderTraversal(Node root, List<List<String>> res) {
        
        if(root == null) {
            return null;
        }
        getInOrderTraversal(root.left, res);
        res.add(Arrays.asList(root.key, root.value));
        getInOrderTraversal(root.right, res);
        return res;
    }
    
}
