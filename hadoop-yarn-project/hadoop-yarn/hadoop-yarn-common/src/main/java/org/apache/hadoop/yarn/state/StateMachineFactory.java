/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.state;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * State machine topology.
 * This object is semantically immutable.  If you have a
 * StateMachineFactory there's no operation in the API that changes
 * its semantic properties.
 *
 * @param <OPERAND> The object type on which this state machine operates.
 * @param <STATE> The state of the entity.
 * @param <EVENTTYPE> The external eventType to be handled.
 * @param <EVENT> The event object.
 */
// TODO: 17/3/24 by zmyer
@Public
@Evolving
final public class StateMachineFactory<OPERAND, STATE extends Enum<STATE>,
    EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    //状态转移节点
    private final TransitionsListNode transitionsListNode;

    //状态机表
    private Map<STATE, Map<EVENTTYPE,
        Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

    //初始化状态
    private STATE defaultInitialState;

    //
    private final boolean optimized;

    /**
     * Constructor
     *
     * This is the only constructor in the API.
     */
    // TODO: 17/3/25 by zmyer
    public StateMachineFactory(STATE defaultInitialState) {
        this.transitionsListNode = null;
        //初始化默认状态
        this.defaultInitialState = defaultInitialState;
        this.optimized = false;
        this.stateMachineTable = null;
    }

    // TODO: 17/3/25 by zmyer
    private StateMachineFactory(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
        ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
        //设置初始化状态
        this.defaultInitialState = that.defaultInitialState;
        //创建状态转移节点
        this.transitionsListNode = new TransitionsListNode(t, that.transitionsListNode);
        this.optimized = false;
        this.stateMachineTable = null;
    }

    // TODO: 17/3/25 by zmyer
    private StateMachineFactory(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
        boolean optimized) {
        //设置初始化状态
        this.defaultInitialState = that.defaultInitialState;
        //设置状态转移列表节点
        this.transitionsListNode = that.transitionsListNode;
        //设置优化标记
        this.optimized = optimized;
        if (optimized) {
            //创建状态机表
            makeStateMachineTable();
        } else {
            stateMachineTable = null;
        }
    }

    // TODO: 17/3/25 by zmyer
    private interface ApplicableTransition<OPERAND, STATE extends Enum<STATE>,
        EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
        void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
    }

    // TODO: 17/3/25 by zmyer
    private class TransitionsListNode {
        //状态转移对象
        final ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;
        //状态转移节点指针
        final TransitionsListNode next;

        // TODO: 17/3/25 by zmyer
        TransitionsListNode(ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition,
            TransitionsListNode next) {
            this.transition = transition;
            this.next = next;
        }
    }

    // TODO: 17/3/25 by zmyer
    static private class ApplicableSingleOrMultipleTransition
        <OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT>
        implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {
        //前一个状态
        final STATE preState;
        //事件类型
        final EVENTTYPE eventType;
        //状态转移对象
        final Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

        // TODO: 17/3/25 by zmyer
        ApplicableSingleOrMultipleTransition(STATE preState, EVENTTYPE eventType,
            Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
            this.preState = preState;
            this.eventType = eventType;
            this.transition = transition;
        }

        // TODO: 17/3/25 by zmyer
        @Override
        public void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
            //根据前一个状态,读取状态映射表
            Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
                = subject.stateMachineTable.get(preState);
            if (transitionMap == null) {
                // I use HashMap here because I would expect most EVENTTYPE's to not
                //  apply out of a particular state, so FSM sizes would be
                //  quadratic if I use EnumMap's here as I do at the top level.
                //如果前一个状态不存在状态映射表,则创建
                transitionMap = new HashMap<>();
                //将创建好的状态映射表注册到状态机表中
                subject.stateMachineTable.put(preState, transitionMap);
            }
            //将状态对象以及事件类型插入到状态转移映射表中
            transitionMap.put(eventType, transition);
        }
    }

    /**
     * @param preState pre-transition state
     * @param postState post-transition state
     * @param eventType stimulus for the transition
     * @return a NEW StateMachineFactory just like {@code this} with the current transition added as a new legal
     * transition.  This overload has no hook object.
     *
     * Note that the returned StateMachineFactory is a distinct object.
     *
     * This method is part of the API.
     */
    // TODO: 17/3/25 by zmyer
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
    addTransition(STATE preState, STATE postState, EVENTTYPE eventType) {
        //状态转移对象插入到状态机中
        return addTransition(preState, postState, eventType, null);
    }

    /**
     * @param preState pre-transition state
     * @param postState post-transition state
     * @param eventTypes List of stimuli for the transitions
     * @return a NEW StateMachineFactory just like {@code this} with the current transition added as a new legal
     * transition.  This overload has no hook object.
     *
     *
     * Note that the returned StateMachineFactory is a distinct object.
     *
     * This method is part of the API.
     */
    // TODO: 17/3/25 by zmyer
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
        STATE preState, STATE postState, Set<EVENTTYPE> eventTypes) {
        //添加状态转移对象
        return addTransition(preState, postState, eventTypes, null);
    }

    /**
     * @param preState pre-transition state
     * @param postState post-transition state
     * @param eventTypes List of stimuli for the transitions
     * @param hook transition hook
     * @return a NEW StateMachineFactory just like {@code this} with the current transition added as a new legal
     * transition
     *
     * Note that the returned StateMachineFactory is a distinct object.
     *
     * This method is part of the API.
     */
    // TODO: 17/3/25 by zmyer
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
    addTransition(STATE preState, STATE postState, Set<EVENTTYPE> eventTypes,
        SingleArcTransition<OPERAND, EVENT> hook) {
        //状态机工厂对象
        StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;
        //依次遍历每个事件类型
        for (EVENTTYPE event : eventTypes) {
            if (factory == null) {
                //将每个事件类型以及状态转移对象插入到状态机中
                factory = addTransition(preState, postState, event, hook);
            } else {
                //将每个事件类型以及状态转移对象插入到状态机中
                factory = factory.addTransition(preState, postState, event, hook);
            }
        }
        //返回状态机工厂对象
        return factory;
    }

    /**
     * @param preState pre-transition state
     * @param postState post-transition state
     * @param eventType stimulus for the transition
     * @param hook transition hook
     * @return a NEW StateMachineFactory just like {@code this} with the current transition added as a new legal
     * transition
     *
     * Note that the returned StateMachineFactory is a distinct object.
     *
     * This method is part of the API.
     */
    // TODO: 17/3/25 by zmyer
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
    addTransition(STATE preState, STATE postState, EVENTTYPE eventType, SingleArcTransition<OPERAND, EVENT> hook) {
        //创建状态机工厂对象
        return new StateMachineFactory<>(this, new ApplicableSingleOrMultipleTransition<>
            (preState, eventType, new SingleInternalArc(postState, hook)));
    }

    /**
     * @param preState pre-transition state
     * @param postStates valid post-transition states
     * @param eventType stimulus for the transition
     * @param hook transition hook
     * @return a NEW StateMachineFactory just like {@code this} with the current transition added as a new legal
     * transition
     *
     * Note that the returned StateMachineFactory is a distinct object.
     *
     * This method is part of the API.
     */
    //将每个事件类型以及状态转移对象插入到状态机中
    public StateMachineFactory
    addTransition(STATE preState, Set<STATE> postStates, EVENTTYPE eventType,
        MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
        //创建状态机工厂对象
        return new StateMachineFactory<>(this, new ApplicableSingleOrMultipleTransition<>
            (preState, eventType, new MultipleInternalArc(postStates, hook)));
    }

    /**
     * @return a StateMachineFactory just like {@code this}, except that if you won't need any synchronization to build
     * a state machine
     *
     * Note that the returned StateMachineFactory is a distinct object.
     *
     * This method is part of the API.
     *
     * The only way you could distinguish the returned StateMachineFactory from {@code this} would be by measuring the
     * performance of the derived {@code StateMachine} you can get from it.
     *
     * Calling this is optional.  It doesn't change the semantics of the factory, if you call it then when you use the
     * factory there is no synchronization.
     */
    // TODO: 17/3/25 by zmyer
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> installTopology() {
        return new StateMachineFactory<>(this, true);
    }

    /**
     * Effect a transition due to the effecting stimulus.
     *
     * @param state current state
     * @param eventType trigger to initiate the transition
     * @param cause causal eventType context
     * @return transitioned state
     */
    // TODO: 17/3/25 by zmyer
    private STATE doTransition(OPERAND operand, STATE oldState, EVENTTYPE eventType, EVENT event)
        throws InvalidStateTransitionException {
        // We can assume that stateMachineTable is non-null because we call
        //  maybeMakeStateMachineTable() when we build an InnerStateMachine ,
        //  and this code only gets called from inside a working InnerStateMachine .
        //从状态机状态表中读取状态转移映射表
        Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
            = stateMachineTable.get(oldState);
        if (transitionMap != null) {
            //根据事件类型,读取状态转移对象
            Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = transitionMap.get(eventType);
            if (transition != null) {
                //开始进行状态转移
                return transition.doTransition(operand, oldState, event, eventType);
            }
        }
        throw new InvalidStateTransitionException(oldState, eventType);
    }

    // TODO: 17/3/25 by zmyer
    private synchronized void maybeMakeStateMachineTable() {
        if (stateMachineTable == null) {
            makeStateMachineTable();
        }
    }

    // TODO: 17/3/25 by zmyer
    private void makeStateMachineTable() {
        Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack =
            new Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>>();

        Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>
            prototype = new HashMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();

        prototype.put(defaultInitialState, null);

        // I use EnumMap here because it'll be faster and denser.  I would
        //  expect most of the states to have at least one transition.
        stateMachineTable = new EnumMap<>(prototype);

        for (TransitionsListNode cursor = transitionsListNode;
            cursor != null;
            cursor = cursor.next) {
            stack.push(cursor.transition);
        }

        while (!stack.isEmpty()) {
            stack.pop().apply(this);
        }
    }

    // TODO: 17/3/25 by zmyer
    private interface Transition<OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>,
        EVENT> {
        // TODO: 17/3/25 by zmyer
        STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType);
    }

    // TODO: 17/3/25 by zmyer
    private class SingleInternalArc implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {

        private STATE postState;
        private SingleArcTransition<OPERAND, EVENT> hook; // transition hook

        // TODO: 17/3/25 by zmyer
        SingleInternalArc(STATE postState, SingleArcTransition<OPERAND, EVENT> hook) {
            this.postState = postState;
            this.hook = hook;
        }

        // TODO: 17/3/25 by zmyer
        @Override
        public STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType) {
            if (hook != null) {
                hook.transition(operand, event);
            }
            return postState;
        }
    }

    // TODO: 17/3/25 by zmyer
    private class MultipleInternalArc
        implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {
        // Fields
        //状态集合对象
        private Set<STATE> validPostStates;
        //状态转移hook对象
        private MultipleArcTransition<OPERAND, EVENT, STATE> hook;  // transition hook

        // TODO: 17/3/25 by zmyer
        MultipleInternalArc(Set<STATE> postStates,
            MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
            this.validPostStates = postStates;
            this.hook = hook;
        }

        // TODO: 17/3/25 by zmyer
        @Override
        public STATE doTransition(OPERAND operand, STATE oldState,
            EVENT event, EVENTTYPE eventType) throws InvalidStateTransitionException {
            //开始进行状态转移
            STATE postState = hook.transition(operand, event);
            //如果状态集合中不包含该状态对象
            if (!validPostStates.contains(postState)) {
                throw new InvalidStateTransitionException(oldState, eventType);
            }
            //返回后一个状态对象
            return postState;
        }
    }

    /*
     * @return a {@link StateMachine} that starts in
     *         {@code initialState} and whose {@link Transition} s are
     *         applied to {@code operand} .
     *
     *         This is part of the API.
     *
     * @param operand the object upon which the returned
     *                {@link StateMachine} will operate.
     * @param initialState the state in which the returned
     *                {@link StateMachine} will start.
     *
     */
    // TODO: 17/3/25 by zmyer
    public StateMachine<STATE, EVENTTYPE, EVENT>
    make(OPERAND operand, STATE initialState) {
        return
            new InternalStateMachine(operand, initialState);
    }

    /*
     * @return a {@link StateMachine} that starts in the default initial
     *          state and whose {@link Transition} s are applied to
     *          {@code operand} .
     *
     *         This is part of the API.
     *
     * @param operand the object upon which the returned
     *                {@link StateMachine} will operate.
     *
     */
    // TODO: 17/3/25 by zmyer
    public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
        return new InternalStateMachine(operand, defaultInitialState);
    }

    // TODO: 17/3/25 by zmyer
    private class InternalStateMachine implements StateMachine<STATE, EVENTTYPE, EVENT> {
        //操作对象
        private final OPERAND operand;
        //当前的状态对象
        private STATE currentState;

        // TODO: 17/3/25 by zmyer
        InternalStateMachine(OPERAND operand, STATE initialState) {
            this.operand = operand;
            this.currentState = initialState;
            if (!optimized) {
                maybeMakeStateMachineTable();
            }
        }

        // TODO: 17/3/25 by zmyer
        @Override
        public synchronized STATE getCurrentState() {
            return currentState;
        }

        // TODO: 17/3/25 by zmyer
        @Override
        public synchronized STATE doTransition(EVENTTYPE eventType, EVENT event)
            throws InvalidStateTransitionException {
            //开始进行状态转移
            currentState = StateMachineFactory.this.doTransition(operand, currentState, eventType, event);
            return currentState;
        }
    }

    /**
     * Generate a graph represents the state graph of this StateMachine
     *
     * @param name graph name
     * @return Graph object generated
     */
    // TODO: 17/3/25 by zmyer
    @SuppressWarnings("rawtypes")
    public Graph generateStateGraph(String name) {
        maybeMakeStateMachineTable();
        Graph g = new Graph(name);
        for (STATE startState : stateMachineTable.keySet()) {
            Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitions
                = stateMachineTable.get(startState);
            for (Entry<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> entry :
                transitions.entrySet()) {
                Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = entry.getValue();
                if (transition instanceof StateMachineFactory.SingleInternalArc) {
                    StateMachineFactory.SingleInternalArc sa
                        = (StateMachineFactory.SingleInternalArc) transition;
                    Graph.Node fromNode = g.getNode(startState.toString());
                    Graph.Node toNode = g.getNode(sa.postState.toString());
                    fromNode.addEdge(toNode, entry.getKey().toString());
                } else if (transition instanceof StateMachineFactory.MultipleInternalArc) {
                    StateMachineFactory.MultipleInternalArc ma
                        = (StateMachineFactory.MultipleInternalArc) transition;
                    for (Object validPostState : ma.validPostStates) {
                        Graph.Node fromNode = g.getNode(startState.toString());
                        Graph.Node toNode = g.getNode(validPostState.toString());
                        fromNode.addEdge(toNode, entry.getKey().toString());
                    }
                }
            }
        }
        return g;
    }
}
