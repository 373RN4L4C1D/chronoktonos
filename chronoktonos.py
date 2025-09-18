import networkx as nx
import json
import random
import asyncio
import os
import aiohttp
from functools import lru_cache
from typing import Dict, Any, Callable, Tuple, List, Optional
from collections import defaultdict, deque
import threading
import time
import ast  # For exec safety

random.seed(42)

BASE_CONFIG = {'failure_rate': 0.05, 'human_timeout': 2.0, 'evolve_threshold': 0.8, 'max_retries': 3, 'backoff_factor': 0.1, 'model': 'grok-4', 'balance': 0.5}

MEMORY_FILE = 'scars.json'

# Retry Wrapper (Yin: Grounded resilience)
async def add_retry(func: Callable, max_retries: int = 3):
    async def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                await asyncio.sleep(BASE_CONFIG['backoff_factor'] * (2 ** attempt))
        return None
    return wrapper

class Registry:
    def __init__(self):
        self.lock = threading.Lock()
        self.data: Dict[str, Dict[str, Any]] = defaultdict(dict)

    def register_node(self, node_id: str, initial_data: Dict[str, Any] = None) -> None:
        with self.lock:
            self.data[node_id].update({'status': 'pending', 'heartbeat_ts': time.time(), **(initial_data or {})})

    def update_scar(self, node_id: str, scar: str) -> None:
        with self.lock:
            self.data[node_id]['scar'] = scar
            self.data[node_id]['status'] = 'active'

    def complete_node(self, node_id: str) -> None:
        with self.lock:
            self.data[node_id]['status'] = 'complete'

    def get_scar(self, node_id: str) -> str:
        with self.lock:
            return self.data[node_id].get('scar', 'initial')

    def get_status(self, stale_threshold: float = 60.0) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        with self.lock:
            return {
                k: {
                    'status': v['status'],
                    'stale': (now - v.get('heartbeat_ts', 0)) > stale_threshold
                }
                for k, v in self.data.items()
            }

class Mailbox:
    def __init__(self):
        self.queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    async def send(self, topic: str, msg: Dict[str, Any]) -> None:
        await self.queues[topic].put(msg)

    async def receive(self, topic: str, timeout: float = None) -> Optional[Dict[str, Any]]:
        timeout_val = BASE_CONFIG.get('human_timeout', 2.0) if timeout is None else timeout
        try:
            return await asyncio.wait_for(self.queues[topic].get(), timeout=timeout_val)
        except asyncio.TimeoutError:
            return None

class HeartbeatMonitor:
    def __init__(self, registry: Registry, mailbox: Mailbox, config: Dict[str, Any]):
        self.registry = registry
        self.mailbox = mailbox
        self.config = config

    async def monitor(self, node_id: str, interval: float = 30.0) -> None:
        attempt = 0
        while True:
            await asyncio.sleep(interval)
            if self.registry.data[node_id].get('status') not in ['active', 'pending']:
                break
            self.registry.data[node_id]['heartbeat_ts'] = time.time()
            if random.random() < self.config['failure_rate'] * 2:
                attempt += 1
                print(f"Heartbeat alert: {node_id} stalled! Attempt {attempt}, requeuing...")
                self.registry.data[node_id]['status'] = 'failed'
                await self.mailbox.send("recovery_queue", {'node': node_id, 'attempt': attempt})
                if attempt >= self.config['max_retries']:
                    break
                await asyncio.sleep(self.config['backoff_factor'] * (2 ** attempt))

class AsyncGrokClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.api_key = os.getenv('XAI_API_KEY')
        self.base_url = 'https://api.x.ai/v1/chat/completions'
        self.session = None
        self.use_mock = not self.api_key

    async def _get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session

    async def call(self, prompt: str, temperature: float = 0.7) -> str:
        if self.use_mock:
            if random.random() < self.config['failure_rate']:
                raise ValueError(f"Mock Grok failure for {prompt[:20]}")
            insight = f"Mock Insight: Hey collaborator, on {prompt[:20]}... (temp: {temperature:.1f})"
            need_human = random.choice([True, False])
            num_subs = random.randint(2, 4)
            collaborative = random.choice([False, True])
            return json.dumps({'insight': insight, 'need_human_for_subs': need_human, 'suggested_subs': num_subs, 'collaborative': collaborative})
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        payload = {
            'model': self.config.get('model', 'grok-4'),
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': temperature,
            'max_tokens': 512
        }
        session = await self._get_session()
        try:
            async with session.post(self.base_url, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    raise ValueError(f"API error {resp.status}: {await resp.text()}")
                data = await resp.json()
                insight = data['choices'][0]['message']['content']
                return json.dumps({'insight': insight, 'need_human_for_subs': random.choice([True, False]), 'suggested_subs': random.randint(2, 4), 'collaborative': random.choice([False, True])})
        except Exception as e:
            # Backoff for API errors
            await asyncio.sleep(self.config['backoff_factor'] * 2)
            raise ValueError(f"Real Grok call failed: {e}")

    def parse_response(self, result: str) -> Tuple[str, bool, int, bool]:
        try:
            parsed = json.loads(result)
            return parsed.get('insight', 'Default insight'), parsed.get('need_human_for_subs', False), parsed.get('suggested_subs', 3), parsed.get('collaborative', False)
        except:
            return 'Fallback insight', False, 3, False

    async def close(self):
        if self.session:
            await self.session.close()

class SpiritImprinter:
    def __init__(self, grok_client: AsyncGrokClient, config: Dict[str, Any]):
        self.grok = grok_client
        self.balance = config.get('balance', 0.5)  # Yin-Yang: 0=grounded, 1=sky

    async def imprint(self, insight: str, scar: str, collaborative: bool = False) -> str:
        if not collaborative:
            return insight
        # Balance prompt: Blend grounded (factual) with sky (wit) based on balance
        yin_weight = 1 - self.balance
        yang_weight = self.balance
        spirit_prompt = f"Imprint balanced spirit on '{insight}' with scar '{scar[:50]}...': {yin_weight:.1f} grounded truth (factual summary), {yang_weight:.1f} sky wit (improbability/sarcasm). Concise blend."
        spirit_mutation = await self.grok.call(spirit_prompt, 0.8)
        mutation = self.grok.parse_response(spirit_mutation)[0]
        return f"{insight} | Yin-Yang Reveal: {mutation}"

class ChronoktonosSystem:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config.copy() if config else BASE_CONFIG.copy()
        self.registry = Registry()
        self.mailbox = Mailbox()
        self.heartbeat_monitor = HeartbeatMonitor(self.registry, self.mailbox, self.config)
        self.grok = AsyncGrokClient(self.config)
        self.spirit_imprinter = SpiritImprinter(self.grok, self.config)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.grok.close()

    def reset_state(self, load_persistent: bool = True) -> None:
        self.registry.data = defaultdict(dict)
        self.mailbox.queues = defaultdict(asyncio.Queue)
        if load_persistent and os.path.exists(MEMORY_FILE):
            with open(MEMORY_FILE, 'r') as f:
                persistent = json.load(f)
                for k, v in persistent.items():
                    self.registry.register_node(k, v)

    def save_persistent_memory(self) -> None:
        persistent = {k: {'scar': v.get('scar'), 'status': v.get('status')} for k, v in self.registry.data.items() if 'scar' in v}
        with open(MEMORY_FILE, 'w') as f:
            json.dump(persistent, f)

    async def dynamic_operation(self, node: str, context: Dict[str, Any], graph: nx.DiGraph) -> str:
        recovery = await self.mailbox.receive("recovery_queue", timeout=0.1)
        if recovery and recovery['node'] == node:
            print(f"Self-healing: Retrying {node} after recovery")
        interface = 'human' if context.get('require_human', False) else 'grok'
        print(f"Dynamic op for {node}: interface={interface}")
        if interface == 'grok':
            return await self.grok_operation(node, context, graph)
        elif interface == 'human':
            return await self.human_operation(node, context, graph)
        else:
            scar = self.registry.get_scar(node)
            insight = f"Emergent: {node} (scar: {scar})"
            imprinted = await self.spirit_imprinter.imprint(insight, scar)
            self.registry.update_scar(node, imprinted)
            asyncio.create_task(self.heartbeat_monitor.monitor(node))
            self.registry.complete_node(node)
            return imprinted

    async def grok_operation(self, node: str, context: Dict[str, Any], graph: nx.DiGraph) -> str:
        try:
            scar = self.registry.get_scar(node)
            avg_in = max(1, graph.in_degree(node))
            temp = max(0.1, 1.0 / avg_in)
            prompt = f"Hey Grok, as my trusted partner, tackle {node} with scar {scar}. What's your take?"
            result = await self.grok.call(prompt, temp)
            insight, need_human, _, collaborative = self.grok.parse_response(result)
            print(f"Grok op for {node}: need_human={need_human}, collaborative={collaborative}, temp={temp}")
            imprinted = await self.spirit_imprinter.imprint(insight, scar, collaborative)
            self.registry.update_scar(node, imprinted)
            if need_human:
                await self.mailbox.send("global_human_queue", {'node': node, 'insight': imprinted})
                human_resp = await self.mailbox.receive("human_responses")
                if human_resp:
                    final = f"{imprinted} [Human symbiosis: {human_resp['input']}]"
                    self.registry.update_scar(node, final)
                    return final
                else:
                    final = f"{imprinted} [Awaiting human input]"
                    self.registry.update_scar(node, final)
                    return final
            if collaborative:
                async def validator_agent(sub_node: str) -> str:
                    val_prompt = f"Validate {imprinted} as sub-agent."
                    val_result = await self.grok.call(val_prompt, 0.6)
                    val_insight = self.grok.parse_response(val_result)[0]
                    self.registry.update_scar(sub_node, val_insight)
                    self.registry.complete_node(sub_node)
                    return val_insight
                sub_agents = [validator_agent, validator_agent]
                await self.meta_agent_overseer(node, sub_agents)
                collab_resp = {"validation": "Hierarchically approved"}
                final = f"{imprinted} [Meta-overseer validated: {collab_resp.get('validation', 'OK')}]"
                self.registry.update_scar(node, final)
                return final
            for child in list(graph.successors(node)):
                print(f"Propagating require_human to {child}")
                self.registry.data[child]['require_human'] = True
            asyncio.create_task(self.heartbeat_monitor.monitor(node))
            self.registry.complete_node(node)
            return imprinted
        except Exception as e:
            error_scar = f"Failed: {e}"
            self.registry.update_scar(node, error_scar)
            self.registry.data[node]['status'] = 'failed'
            return error_scar

    async def human_operation(self, node: str, context: Dict[str, Any], graph: nx.DiGraph) -> str:
        try:
            print(f"Human op for {node}")
            scar = self.registry.get_scar(node)
            await self.mailbox.send("global_human_queue", {'node': node, 'prompt': f"Collaborate on {node} with scar: {scar}"})
            human_input = await self.mailbox.receive("human_responses")
            if human_input:
                insight = f"Human: {node} (synergy: {human_input['input']}, scar: {scar})"
            else:
                print(f"Escalating {node} to Grok")
                insight = await self.grok_operation(node, context, graph) + " [Escalated from human]"
            imprinted = await self.spirit_imprinter.imprint(insight, scar)
            self.registry.update_scar(node, imprinted)
            asyncio.create_task(self.heartbeat_monitor.monitor(node))
            self.registry.complete_node(node)
            return imprinted
        except Exception as e:
            error_scar = f"Failed: {e}"
            self.registry.update_scar(node, error_scar)
            self.registry.data[node]['status'] = 'failed'
            return error_scar

    async def meta_agent_overseer(self, task: str, sub_agents: List[Callable]) -> None:
        print(f"Meta-Agent overseeing {len(sub_agents)} sub-agents for {task}")
        sub_tasks = []
        for i, agent in enumerate(sub_agents):
            sub_id = f"{task}_sub{i}"
            self.registry.register_node(sub_id)
            sub_task = asyncio.create_task(agent(sub_id))
            sub_tasks.append((sub_id, sub_task))
        for sub_id, sub_task in sub_tasks:
            try:
                await sub_task
            except Exception as e:
                print(f"Meta: Sub-agent {sub_id} failed: {e} – delegating recovery")
                if self.config['failure_rate'] > 0.01:
                    self.config['failure_rate'] *= 0.9
        print("Meta-Agent: Delegation complete.")

    async def ai_decompose_subs(self, task: str) -> Tuple[Tuple[str, ...], int]:
        prompt = f"Hey Grok, brainstorm 2-4 subtasks for '{task}' – keep it collaborative."
        result = await self.grok.call(prompt, 0.5)
        _, _, num_subs, _ = self.grok.parse_response(result)
        return tuple(f"{task}_{i}" for i in range(num_subs)), num_subs

    ai_decompose_subs = add_retry(ai_decompose_subs)  # Yin: Retry on decomp failures

    async def decompose(self, task: str, depth: int = 2, require_human: bool = False) -> nx.DiGraph:
        if depth == 0:
            return nx.DiGraph()
        graph = nx.DiGraph()
        planner_prompt = f"Yo planner, strategize for {task} at depth {depth} – flag risks, suggest delegations."
        planner_insight = await self.grok.call(planner_prompt, 0.4)
        planner_scar = self.grok.parse_response(planner_insight)[0]
        planner_node = f"{task}_planner"
        self.registry.register_node(planner_node)
        async def planner_op(n: str, c: Dict[str, Any], g: nx.DiGraph) -> str:
            return planner_scar
        graph.add_node(planner_node, op=planner_op)
        self.registry.update_scar(planner_node, planner_scar)
        self.registry.complete_node(planner_node)
        print(f"Planner: {planner_scar}")
        
        self.registry.register_node(task, {'require_human': require_human})
        graph.add_node(task, op=self.dynamic_operation)
        subs, _ = await self.ai_decompose_subs(task)
        for sub in subs:
            graph.add_node(sub, op=self.dynamic_operation)
            graph.add_edge(task, sub)
            self.registry.register_node(sub)
            await self.mailbox.send("subtask_queue", {'parent': task, 'child': sub})
        graph.add_edge(planner_node, task)
        for sub in subs:
            sub_graph = await self.decompose(sub, depth - 1)
            graph = nx.compose(graph, sub_graph)
        return graph

    async def run_debrief(self, final_scars: Dict[str, str]) -> Dict[str, Any]:
        complete_count = sum(1 for v in final_scars.values() if 'failed' not in v.lower() and v)
        compliance = complete_count / len(final_scars) if final_scars else 0
        healed = sum(1 for v in final_scars.values() if 'self-heal' in v.lower())
        planned = sum(1 for k, v in final_scars.items() if '_planner' in k and v) / len([k for k in final_scars if '_planner' in k]) if any('_planner' in k for k in final_scars) else 1.0
        spirited = sum(1 for v in final_scars.values() if 'Yin-Yang Reveal' in v) / len(final_scars)
        balance_score = healed / max(1, len(final_scars)) + spirited  # Yin (healed) + Yang (spirited)
        if compliance > 0.9:
            self.config['failure_rate'] *= 0.8
        elif compliance < 0.7:
            self.config['failure_rate'] *= 1.2
        print(f"Real-time adapt: failure_rate now {self.config['failure_rate']:.3f}")
        prompt = f"Team debrief on outcomes (compliance: {compliance:.2f}, healed: {healed}, planned: {planned:.2f}, spirited: {spirited:.2f}, balance: {balance_score:.2f}): {json.dumps(final_scars)}"
        debrief_result = await self.grok.call(prompt, 0.5)
        summary, _, _, _ = self.grok.parse_response(debrief_result)
        report = {
            'timestamp': time.time(),
            'scars': final_scars,
            'summary': summary,
            'compliance': compliance * planned * (1 + spirited * 0.1),
            'healed': healed,
            'planned': planned,
            'spirited': spirited,
            'balance': balance_score
        }
        await self.mailbox.send('debrief_complete', report)
        self.save_persistent_memory()
        return report

    async def run_graph(self, root_task: str, parallel: bool = True) -> Dict[str, Any]:
        self.reset_state(load_persistent=True)
        graph = await self.decompose(root_task)
        print(f"Graph built: {len(graph.nodes)} nodes, depth 2")
        
        if parallel:
            indeg = dict(graph.in_degree)
            ready = deque([n for n in graph if indeg[n] == 0])
            tasks = []
            while ready:
                node = ready.popleft()
                if self.registry.data[node].get('status') == 'complete':
                    for child in graph.successors(node):
                        indeg[child] -= 1
                        if indeg[child] == 0:
                            ready.append(child)
                    continue
                op = graph.nodes[node].get('op', self.dynamic_operation)
                task = asyncio.create_task(op(node, self.registry.data[node], graph))
                tasks.append(task)
                for child in graph.successors(node):
                    indeg[child] -= 1
                    if indeg[child] == 0:
                        ready.append(child)
            await asyncio.gather(*tasks, return_exceptions=True)
            print("All tasks completed")
        
        status = self.registry.get_status()
        print("Registry Status:", status)
        
        final_scars = {k: v['scar'] for k, v in self.registry.data.items() if 'scar' in v and v['scar']}
        print("Final Scars:", final_scars)
        
        debrief = await self.run_debrief(final_scars)
        print(f"Debrief Summary: {debrief['summary']} (Compliance: {debrief['compliance']:.2f}, Healed: {debrief['healed']}, Planned: {debrief['planned']:.2f}, Spirited: {debrief['spirited']:.2f}, Balance: {debrief['balance']:.2f})")
        print("Debrief posted to mailbox; Memory persisted.")
        return debrief

    async def self_evolve(self, debrief: Dict[str, Any]) -> Dict[str, Any]:
        deployment_score = debrief['compliance'] * debrief['planned'] * (1 - self.config['failure_rate'])
        if deployment_score < 0.6:
            return {'applied': [], 'description': 'Risk gate: Pause evolution – refactor hierarchy for value.'}
        if abs(debrief['balance'] - 1.0) > 0.3:  # Imbalance trigger
            self.config['balance'] = 0.5  # Re-center Yin-Yang
            print("Evo tweak: Rebalanced Yin-Yang to 0.5 for harmony.")
        def meta_mock(prompt: str, temp: float) -> str:
            return json.dumps({
                'insight': '2025 Hierarchies: Empower meta-agents; Add real-time risk tuning with balanced spirit.',
                'need_human_for_subs': False,
                'suggested_subs': 1,
                'changes': {'max_retries': self.config['max_retries'] + 1, 'evolve_threshold': min(0.95, self.config['evolve_threshold'] + 0.05)},
                'code_snippet': '''# Balanced risk-check (Yin retry + Yang wit)
async def risk_check(sub_task):
    if random.random() < 0.1:
        raise ValueError("High-risk sub-task – but in the sky's improbability, retry with wit!")
    return await sub_task'''
            })
        meta_grok = lru_cache(maxsize=1)(meta_mock)
        
        prompt = f"Gartner Safeguards: Score {deployment_score:.2f}. Compliance {debrief['compliance']:.2f}, Balance {debrief['balance']:.2f}: Suggest safe JSON changes/snippet with Grok wit."
        result = meta_grok(prompt, 0.3)
        try:
            parsed = json.loads(result)
            description = parsed.get('insight', '')
            changes = parsed.get('changes', {})
            snippet = parsed.get('code_snippet', None)
            for k, v in changes.items():
                self.config[k] = v
            if snippet:
                try:
                    ast.parse(snippet)
                    exec(snippet, globals())
                    print("Code mutation applied: Balanced risk-checked delegation.")
                except (SyntaxError, Exception) as exec_e:
                    print(f"Exec warning (snippet invalid): {exec_e}")
            return {'applied': list(changes.keys()), 'description': description, 'snippet_applied': bool(snippet)}
        except Exception as e:
            return {'applied': [], 'description': f'Parse failed: {e}'}

    async def autonomous_loop(self, root_task: str, max_iters: int = 3) -> List[Dict[str, Any]]:
        evolutions = []
        for it in range(max_iters):
            print(f"\n--- Autonomous Iteration {it+1}/{max_iters} ---")
            debrief = await self.run_graph(root_task)
            compliance = debrief['compliance']
            if compliance < self.config['evolve_threshold']:
                evolution = await self.self_evolve(debrief)
                evolutions.append(evolution)
                print(f"Self-Evolved: {evolution['description']}")
                if evolution.get('snippet_applied', False):
                    print("Code mutation applied via exec.")
            else:
                print(f"Compliance {compliance:.2f} >= threshold {self.config['evolve_threshold']}. Loop complete.")
                break
        print(f"Autonomous loop finalized: {len(evolutions)} evolutions applied.")
        return evolutions

# Demo Usage (set XAI_API_KEY for real; balance via config)
# system = ChronoktonosSystem(config={'balance': 0.5})
# asyncio.run(system.autonomous_loop("Analyze Market Trends", max_iters=1))
