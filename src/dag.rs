use crate::structs::State;
use crate::Result;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Vertex {
    children: HashSet<usize>,
    parents: HashSet<usize>,
    pub state: State,
    parents_outstanding: usize,
}

impl Vertex {
    fn new() -> Self {
        Vertex {
            children: HashSet::new(),
            parents: HashSet::new(),
            state: State::Queued,
            parents_outstanding: 0,
        }
    }
}

#[derive(Debug)]
pub struct DAG {
    pub vertices: Vec<Vertex>,
    ready: HashSet<usize>,
    visiting: HashSet<usize>,
}

impl DAG {
    pub fn new() -> Self {
        DAG {
            vertices: vec![],
            ready: HashSet::new(),
            visiting: HashSet::new(),
        }
    }

    pub fn add_vertices(&mut self, n: usize) {
        let s = self.vertices.len();
        self.vertices.resize(s + n, Vertex::new());
        for i in s..n {
            self.ready.insert(i);
        }
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

    pub fn set_vertex_state(&mut self, id: usize, state: State) -> Result<()> {
        let cur_state = self.vertices[id].state;

        if cur_state == state {
            return Ok(());
        }

        match (cur_state, state) {
            (_, State::Completed) => {
                self.ready.remove(&id);
                self.visiting.remove(&id);
                self.complete_visit(id, false);
            }
            (State::Errored, State::Queued) | (State::Killed, State::Queued) => {
                self.ready.insert(id);
            }
            (_, State::Errored) | (_, State::Killed) => {
                self.ready.remove(&id);
                self.visiting.remove(&id);
                self.complete_visit(id, true);
            }
            (_, _) => {
                return Err(anyhow!(
                    "Unsupported transition from {:?} to {:?}",
                    cur_state,
                    state
                ));
            }
        }
        self.vertices[id].state = state;
        Ok(())
    }

    pub fn add_edge(&mut self, src: usize, dst: usize) -> Result<()> {
        if self.has_path(dst, src) {
            return Err(anyhow!("Adding edge would result in a loop"));
        }
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

    fn has_path(&self, src: usize, dst: usize) -> bool {
        let mut seen = HashSet::<usize>::new();
        return self._has_path(src, dst, &mut seen);
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

    pub fn visit_next(&mut self) -> Option<usize> {
        if self.ready.is_empty() {
            None
        } else {
            let id = self.ready.iter().next().unwrap().clone();
            self.vertices[id].state = State::Running;
            self.ready.take(&id);
            self.visiting.insert(id);
            Some(id)
        }
    }

    pub fn complete_visit(&mut self, id: usize, errored: bool) {
        self.visiting.take(&id);
        match self.vertices[id].state {
            State::Completed => return,
            _ => {}
        }

        if errored {
            self.vertices[id].state = State::Errored;
        } else {
            self.vertices[id].state = State::Completed;
            let children = self.vertices[id].children.clone();
            for child in children.iter() {
                self.vertices[*child].parents_outstanding -= 1;
                if self.vertices[*child].parents_outstanding == 0 {
                    self.ready.insert(*child);
                }
            }
        }
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
        let mut dag = DAG::new();

        dag.add_vertices(3);
        assert_eq!(dag.len(), 3);

        dag.add_vertices(7);
        assert_eq!(dag.len(), 10);
    }

    #[test]
    fn dag_cycle_detection() {
        let mut dag = DAG::new();
        dag.add_vertices(3);
        dag.add_edge(0, 1).unwrap();
        assert!(dag.add_edge(1, 2).is_ok());
        assert!(dag.add_edge(2, 0).is_err());
    }

    #[test]
    fn dag_traversal_order() {
        let mut dag = DAG::new();
        dag.add_vertices(10);

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
            dag.add_edge(*src, *dst).unwrap();
        }
        dag.reset();

        let mut visit_order: Vec<usize> = Vec::new();
        visit_order.resize(dag.len(), 0);
        let mut i: usize = 0;
        loop {
            match dag.visit_next() {
                Some(id) => {
                    dag.complete_visit(id, false);
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
        dag.add_vertices(10);

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
            dag.add_edge(*src, *dst).unwrap();
        }
        dag.reset();

        // At the visit of item 5, we'll add in 3 new vertices with these
        // extra edges:
        let n_extra_vertices: usize = 3;
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
                dag.add_vertices(n_extra_vertices);
                for (src, dst) in extra_edges.iter() {
                    dag.add_edge(*src, *dst).unwrap();
                }
            }

            match dag.visit_next() {
                Some(id) => {
                    dag.complete_visit(id, false);
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
