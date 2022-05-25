use crate::structs::Parameters;
use std::collections::HashSet;
use std::hash::Hash;

// Return the set of variables that are found in the template
pub fn find_applicable_vars<S: AsRef<str> + Hash + Eq>(
    template: &[S],
    variables: &[S],
) -> HashSet<String> {
    let mut found = HashSet::new();
    for var in variables.iter() {
        for part in template.iter() {
            if part.as_ref().find(var.as_ref()).is_some() {
                let val: String = var.as_ref().to_owned();
                found.insert(val);
                break;
            }
        }
    }

    found
}

/// Produces the cartesian product of vectors
fn cartesian_product<T: Clone>(input: &Vec<Vec<T>>) -> Vec<Vec<T>> {
    let mut it = input.iter();
    let mut cur = it
        .next()
        .unwrap()
        .iter()
        .map(|x| vec![x.clone()])
        .collect::<Vec<Vec<T>>>();

    for next in it {
        let mut new_cur = Vec::new();
        for nt in next.iter() {
            for c in cur.iter() {
                let mut tmp = c.clone();
                tmp.push(nt.clone());
                new_cur.push(tmp);
            }
        }
        cur = new_cur;
    }

    cur
}

pub fn generate_interpolation_sets(
    variables: &Parameters,
    subset: &HashSet<String>,
) -> Vec<Vec<(String, String)>> {
    let mut vals = Vec::new();
    let mut keys = Vec::new();

    // Extract the variables that apply
    for (k, v) in variables.iter() {
        if subset.contains(k) {
            vals.push(v.to_vec());
            keys.push(k);
        }
    }

    // Generate the cartesian product of the variable values
    cartesian_product(&vals)
        .iter()
        .map(|x| {
            let mut v = x
                .iter()
                .zip(&keys)
                .map(|(a, b)| (b.to_string(), a.to_string()))
                .collect::<Vec<(String, String)>>();
            v.sort();
            v
        })
        .collect()
}

// Flatten
pub fn apply_vars(
    template: &Vec<String>,
    interpolation_sets: &Vec<Vec<(String, String)>>,
) -> Vec<Vec<String>> {
    // Generate the sets
    interpolation_sets
        .iter()
        .map(|set| {
            template
                .iter()
                .map(|part| {
                    set.iter()
                        .fold(part.clone(), |p, (var, val)| p.replace(var, val))
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_expansion_single() {
        let input = vec!["Hello".to_owned(), "FIRST_NAME".to_owned()];
        let mut vars: Parameters = Parameters::new();
        vars.insert(
            "FIRST_NAME".to_owned(),
            vec!["ABCD".to_owned(), "EFGH".to_owned()],
        );

        let keys = vars
            .keys()
            .into_iter()
            .map(|x| String::from(x))
            .collect::<Vec<String>>();

        let app_vars = find_applicable_vars(&input, &keys);
        let int_sets = generate_interpolation_sets(&vars, &app_vars);
        let result = apply_vars(&input, &int_sets);

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn var_expansion_multiple() {
        let input = vec![
            "Hello,".to_owned(),
            "{{FIRST_NAME}}".to_owned(),
            "we saw you on {{DATE}}".to_owned(),
            "{{FIRST_NAME}}, {{DATE}}".to_owned(),
        ];
        let mut vars: Parameters = Parameters::new();
        vars.insert(
            "{{FIRST_NAME}}".to_owned(),
            vec!["ABCD".to_owned(), "DEFG".to_owned()],
        );
        vars.insert(
            "{{DATE}}".to_owned(),
            vec!["2020-01-01".to_owned(), "2021-01-01".to_owned()],
        );
        vars.insert(
            "{{OTHER}}".to_owned(),
            vec!["apple".to_owned(), "oranges".to_owned()],
        );

        let keys = vars
            .keys()
            .into_iter()
            .map(|x| String::from(x))
            .collect::<Vec<String>>();

        let app_vars = find_applicable_vars(&input, &keys);
        let int_sets = generate_interpolation_sets(&vars, &app_vars);
        let mut result = apply_vars(&input, &int_sets);

        let mut expected_result = vec![
            vec![
                "Hello,".to_owned(),
                "ABCD".to_owned(),
                "we saw you on 2020-01-01".to_owned(),
                "ABCD, 2020-01-01".to_owned(),
            ],
            vec![
                "Hello,".to_owned(),
                "ABCD".to_owned(),
                "we saw you on 2021-01-01".to_owned(),
                "ABCD, 2021-01-01".to_owned(),
            ],
            vec![
                "Hello,".to_owned(),
                "DEFG".to_owned(),
                "we saw you on 2020-01-01".to_owned(),
                "DEFG, 2020-01-01".to_owned(),
            ],
            vec![
                "Hello,".to_owned(),
                "DEFG".to_owned(),
                "we saw you on 2021-01-01".to_owned(),
                "DEFG, 2021-01-01".to_owned(),
            ],
        ];

        expected_result.sort();
        result.sort();
        assert_eq!(result, expected_result);
    }
}
