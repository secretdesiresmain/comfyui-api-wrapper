class ExecutionFailure(Exception):
    pass

def classify_history(history, prompt_id):
    item=history.get(prompt_id) if isinstance(history,dict) else None
    if not isinstance(item,dict):return 'pending'
    status=item.get('status',{})
    messages=status.get('messages',[])
    if status.get('status_str')=='error' or any(isinstance(m,list) and m and m[0] in ('execution_error','execution_interrupted') for m in messages):
        raise ExecutionFailure('execution_failed')
    if status.get('completed') is not True:return 'pending'
    outputs=item.get('outputs',{})
    usable=any(isinstance(node,dict) and any(isinstance(node.get(kind),list) and any(isinstance(output,dict) and output.get('filename') for output in node[kind]) for kind in ('images','gifs','videos','audio')) for node in outputs.values())
    if not usable:raise ExecutionFailure('empty_output')
    return 'completed'
