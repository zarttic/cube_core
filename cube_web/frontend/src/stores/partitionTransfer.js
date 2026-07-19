let pendingSelection = null;

export function queuePartitionSelection(selection) {
  pendingSelection = selection;
}

export function takePartitionSelection() {
  const selection = pendingSelection;
  pendingSelection = null;
  return selection;
}
