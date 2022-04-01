use crate::structs::State;
use crate::Result;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub struct Vertex<T> {
    id: T,
    children: HashSet<usize>,
    parents: HashSet<usize>,
    pub state: State,
    parents_outstanding: usize,
}

impl<T> Vertex<T> {
    fn new(id: T) -> Self {
        Vertex {
            id,
            children: HashSet::new(),
            parents: HashSet::new(),
            state: State::Queued,
            parents_outstanding: 0,
        }
    }
}

#[derive(Debug)]
pub struct DAG<T: Hash + PartialEq + Eq + Clone + Debug> {
    pub vertices: Vec<Vertex<T>>,
    keymap: HashMap<T, usize>,
    ready: HashSet<usize>,
    visiting: HashSet<usize>,
}

impl<T> DAG<T>
where
    T: Hash + PartialEq + Eq + Clone + Debug,
{
    pub fn new() -> Self {
        DAG {
            vertices: Vec::new(),
            keymap: HashMap::new(),
            ready: HashSet::new(),
            visiting: HashSet::new(),
        }
    }

    pub fn add_vertex(&mut self, key: T) -> Result<()> {
        if self.keymap.contains_key(&key) {
            Err(anyhow!("DAG already contains a vertex with key {:?}", key))
        } else {
            self.keymap.insert(key.clone(), self.vertices.len());
            self.vertices.push(Vertex::new(key));
            Ok(())
        }
    }

    pub fn add_vertices(&mut self, keys: &Vec<T>) -> Result<()> {
        for key in keys.iter() {
            self.add_vertex(key.clone())?
        }
        Ok(())
    }

    pub fn reset(&mut self) {
        // Update dependency counts
        for (i, v) in self.vertices.iter_mut().enumerate() {
            v.parents_outstanding = v.parents.len();
            if v.parents_outstanding == 0 {
                self.ready.insert(i);
            }
        }
    }

    pub fn len(&mut self) -> usize {
        self.vertices.len()
    }

    pub fn set_vertex_state(&mut self, key: T, state: State) -> Result<()> {
        let idx = *self.keymap.get(&key).ok_or(anyhow!("No such key"))?;
        let cur_state = self.vertices[idx].state;

        if cur_state == state {
            return Ok(());
        }

        match (cur_state, state) {
            (_, State::Completed) => {
                self.ready.remove(&idx);
                self.visiting.remove(&idx);
                self.complete_visit(&key, false)?;
            }
            (State::Errored, State::Queued) | (State::Killed, State::Queued) => {
                self.ready.insert(idx);
            }
            (_, State::Errored) | (_, State::Killed) => {
                self.ready.remove(&idx);
                self.visiting.remove(&idx);
                self.complete_visit(&key, true)?;
            }
            (_, _) => {
                return Err(anyhow!(
                    "Unsupported transition from {:?} to {:?}",
                    cur_state,
                    state
                ));
            }
        }
        self.vertices[idx].state = state;
        Ok(())
    }

    pub fn add_edge(&mut self, src_key: &T, dst_key: &T) -> Result<()> {
        if self.has_path(dst_key, src_key)? {
            return Err(anyhow!("Adding edge would result in a loop"));
        }
        let src = *self.keymap.get(src_key).unwrap();
        let dst = *self.keymap.get(dst_key).unwrap();
        self.vertices[src].children.insert(dst);
        self.vertices[dst].parents.insert(src);
        match self.vertices[src].state {
            State::Completed => {}
            _ => {
                self.vertices[dst].parents_outstanding += 1;
            }
        }
        if self.vertices[dst].parents_outstanding == 0 {
            self.ready.insert(dst);
        } else {
            self.ready.take(&dst);
        }
        return Ok(());
    }

    fn has_path(&self, src_key: &T, dst_key: &T) -> Result<bool> {
        let src = *self.keymap.get(src_key).ok_or(anyhow!("No such key"))?;
        let dst = *self.keymap.get(dst_key).ok_or(anyhow!("No such key"))?;
        let mut seen = HashSet::<usize>::new();
        return Ok(self._has_path(src, dst, &mut seen));
    }

    fn _has_path(&self, src: usize, dst: usize, seen: &mut HashSet<usize>) -> bool {
        if src == dst {
            return true;
        }
        if seen.contains(&src) {
            return false;
        }
        if self.vertices[src].children.contains(&dst) {
            return true;
        }
        seen.insert(src);
        for child in self.vertices[src].children.iter() {
            if self._has_path(*child, dst, seen) {
                return true;
            }
        }
        return false;
    }

    pub fn visit_next(&mut self) -> Option<T> {
        if self.ready.is_empty() {
            None
        } else {
            let idx = self.ready.iter().next().unwrap().clone();
            self.vertices[idx].state = State::Running;
            self.ready.take(&idx);
            self.visiting.insert(idx);
            Some(self.vertices[idx].id.clone())
        }
    }

    pub fn complete_visit(&mut self, key: &T, errored: bool) -> Result<()> {
        let idx = *self.keymap.get(key).ok_or(anyhow!("No such key"))?;
        self.visiting.take(&idx);
        match self.vertices[idx].state {
            State::Completed => return Ok(()),
            _ => {}
        }

        if errored {
            self.vertices[idx].state = State::Errored;
        } else {
            self.vertices[idx].state = State::Completed;
            let children = self.vertices[idx].children.clone();
            for child in children.iter() {
                self.vertices[*child].parents_outstanding -= 1;
                if self.vertices[*child].parents_outstanding == 0 {
                    self.ready.insert(*child);
                }
            }
        }
        Ok(())
    }

    /// Is there any progress still to be had
    pub fn can_progress(&self) -> bool {
        !(self.ready.is_empty() && self.visiting.is_empty())
    }

    /// Has everything been successfully visited
    pub fn is_complete(&self) -> bool {
        let unvisited = self.vertices.iter().position(|v| match v.state {
            State::Completed => false,
            _ => true,
        });
        match unvisited {
            Some(_idx) => false,
            None => true,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn dag_construction() {
        let mut dag = DAG::<usize>::new();

        dag.add_vertices(&vec![0, 1, 2])
            .expect("Unable to add vertices");
        assert_eq!(dag.len(), 3);

        dag.add_vertices(&vec![3, 4, 5, 6, 7, 8, 9])
            .expect("Unable to add vertices");
        assert_eq!(dag.len(), 10);

        // Unable to add an existing vertex
        assert!(dag.add_vertices(&vec![3]).is_err());
    }

    #[test]
    fn dag_cycle_detection() {
        let mut dag = DAG::<usize>::new();
        dag.add_vertices(&vec![0, 1, 2])
            .expect("Unable to add vertices");
        dag.add_edge(&0, &1).unwrap();
        assert!(dag.add_edge(&1, &2).is_ok());
        assert!(dag.add_edge(&2, &0).is_err());
    }

    #[test]
    fn dag_traversal_order() {
        let mut dag = DAG::new();
        dag.add_vertices(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            .unwrap();

        /*
           0 ---------------------\
           1 ------------ \        \           /-----> 8
           2 ---- 3 ---- > 5 -----> 6 -----> 7
           4 -------------------------------/  \-----> 9
        */
        let edges = [
            (0usize, 6usize),
            (1, 5),
            (2, 3),
            (3, 5),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
            (7, 9),
            (4, 7),
        ];

        for (src, dst) in edges.iter() {
            dag.add_edge(src, dst).unwrap();
        }
        dag.reset();

        let mut visit_order: Vec<usize> = Vec::new();
        visit_order.resize(dag.len(), 0);
        let mut i: usize = 0;
        loop {
            match dag.visit_next() {
                Some(id) => {
                    dag.complete_visit(&id, false)
                        .expect("Unable to complete visit");
                    assert_eq!(visit_order[id], 0);
                    visit_order[id] = i;
                }
                None => break,
            }
            i += 1;
        }

        // All vertices visited
        assert!(dag.is_complete());
        assert_eq!(visit_order.len(), dag.len());
        for (src, dst) in edges.iter() {
            assert!(visit_order[*src] < visit_order[*dst]);
        }
    }

    #[test]
    fn dag_additions_during_traversal() {
        let mut dag = DAG::new();
        dag.add_vertices(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            .unwrap();

        /*
           0 ---------------------\
           1 ------------ \        \           /-----> 8
           2 ---- 3 ---- > 5 -----> 6 -----> 7
           4 -------------------------------/  \-----> 9
        */
        let edges = [
            (0usize, 6usize),
            (1, 5),
            (2, 3),
            (3, 5),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
            (7, 9),
            (4, 7),
        ];

        for (src, dst) in edges.iter() {
            dag.add_edge(src, dst).unwrap();
        }
        dag.reset();

        // At the visit of item 5, we'll add in 3 new vertices with these
        // extra edges:
        let extra_vertices = vec![10, 11, 12];
        let n_extra_vertices: usize = extra_vertices.len();
        let extra_edges = [
            // Adding on to the end
            (7usize, 10usize),
            (10, 8),
            (10, 9),
            // Adding into the middle
            (5, 11),
            (6, 11),
            // Adding in a dependency that's already been visited
            (4, 12),
        ];

        let mut visit_order: Vec<usize> = Vec::new();
        visit_order.resize(dag.len() + n_extra_vertices, 0);
        let mut i: usize = 0;
        loop {
            if i == 5 {
                dag.add_vertices(&extra_vertices)
                    .expect("unable to add vertices");
                for (src, dst) in extra_edges.iter() {
                    dag.add_edge(src, dst).unwrap();
                }
            }

            match dag.visit_next() {
                Some(id) => {
                    dag.complete_visit(&id, false)
                        .expect("unable to complete visit");
                    assert_eq!(visit_order[id], 0);
                    visit_order[id] = i;
                }
                None => break,
            }
            i += 1;
        }

        // All vertices visited
        assert_eq!(visit_order.len(), dag.len());
        for (src, dst) in edges.iter() {
            assert!(visit_order[*src] < visit_order[*dst]);
        }
        for (src, dst) in extra_edges.iter() {
            assert!(visit_order[*src] < visit_order[*dst]);
        }
    }
}
