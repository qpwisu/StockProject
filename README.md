# StockProject
- 프로젝트 설명
    - 웹 프로젝트에서 사용할 주식 정보를 mysql에 업로드
    - 관련주를 찾기 위해 종목들간의 상관계수를 mysql에 업로드
- 기능
    - 파이썬으로 pykrx api를 이용해 한국 주식 가격을 가져와 mysql에 저장
    - 각 종목들간의 pearson 상관계수를 구해 관련주를 찾아 mysql에 저장

- 주요한점
    - pykrx api에서 주식 종목 2000여개의 요청을 해야하는데 이때 멀티 쓰레드를 이용해서 빠르게 처리

```python
#멀티 쓰레드 처리
#날짜를 나눠서 쓰레드별로 처리
def list_chunk(lst, n):
    return [lst[i:i + len(lst)//n] for i in range(0, len(lst), len(lst)//n)]
#사용할 쓰레드 갯수 지정
num_thread = 8
with futures.ThreadPoolExecutor() as executor:
    sub_routine = list_chunk(tickers,num_thread)
    results = executor.map(get_stock, sub_routine)
```
