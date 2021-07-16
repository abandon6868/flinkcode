package com.atguigu.day12;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SumCount {
    private Long sum = 0L;
    private Long count =0L;
}
