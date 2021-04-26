# API documentation

A thorough documentation of the RESTful API routes, responses and types can be accessed through the Swagger docs that the backend serves.
These are accessible at `example.com/api/doc`

## Examples

```
/flows/HelloFlow/runs?_page=4                               List page 4
/flows/HelloFlow/runs?_page=2&_limit=10                     List page 4, each page contains 10 items

/flows/HelloFlow/runs?_order=run_number                     Order by `run_number` in descending order
/flows/HelloFlow/runs?_order=+run_number                    Order by `run_number` in ascending order
/flows/HelloFlow/runs?_order=-run_number                    Order by `run_number` in descending order
/flows/HelloFlow/runs?_order=run_number,ts_epoch            Order by `run_number` and `ts_epoch` in descending order

/runs?_tags=user:dipper                                     Filter by one tag
/runs?_tags=user:dipper,runtime:dev                         Filter by multiple tags (AND)
/runs?_tags:all=user:dipper,runtime:dev                     Filter by multiple tags (AND)
/runs?_tags:any=user:dipper,runtime:dev                     Filter by multiple tags (OR)
/runs?_tags:likeall=user:dip,untime:de                      Filter by multiple tags that contains string (AND)
/runs?_tags:likeany=user:,untime:de                         Filter by multiple tags that contains string (OR)

/runs?_group=flow_id                                        Group by `flow_id`
/runs?_group=flow_id,user_name                              Group by `flow_id` and `user_name`
/runs?_group=user_name&_limit=2                             Group by `user_name` and limit each group to `2` runs
/runs?_group=flow_id&_order=flow_id,run_number              Group by `flow_id` and order by `flow_id & run_number`
/runs?_group=flow_id&user_name=dipper                       List runs by `dipper` and group by `flow_id`
/runs?user=null                                             `user` is NULL

/flows/HelloFlow/runs?run_number=40                         `run_number` equals `40`
/flows/HelloFlow/runs?run_number:eq=40                      `run_number` equals `40`
/flows/HelloFlow/runs?run_number:ne=40                      `run_number` not equals `40`
/flows/HelloFlow/runs?run_number:lt=40                      `run_number` less than `40`
/flows/HelloFlow/runs?run_number:le=40                      `run_number` less than or equals `40`
/flows/HelloFlow/runs?run_number:gt=40                      `run_number` greater than `40`
/flows/HelloFlow/runs?run_number:ge=40                      `run_number` greater than equals `40`

/flows/HelloFlow/runs?user_name:co=atia                     `user_name` contains `atia`
/flows/HelloFlow/runs?user_name:sw=mati                     `user_name` starts with `mati`
/flows/HelloFlow/runs?user_name:ew=tias                     `user_name` ends with `tias`

/flows?user_name=dipper,mabel                               `user_name` is either `dipper` OR `mabel`

/flows/HelloFlow/runs?run_number:lt=60&run_number:gt=40     `run_number` less than 60 and greater than 40
```

## Available operators

| URL operator | Description             | SQL operator |
|--------------|-------------------------|--------------|
| `eq`         | equals                  | `=`          |
| `ne`         | not equals              | `!=`         |
| `lt`         | less than               | `<`          |
| `le`         | less than equals        | `<=`         |
| `gt`         | greater than            | `>`          |
| `ge`         | greater than equals     | `>=`         |
| `co`         | contains                | `*string*`   |
| `sw`         | starts with             | `^string*`   |
| `ew`         | ends with               | `*string$`   |
| `is`         | is                      | `IS`         |
| `li`         | is like (use with %val) | `ILIKE`      |