from typing import Dict, List


class PlotHelper:
    _color_dict: Dict[str, str]
    _symbol_dict: Dict[str, str]
    _colors: List[str]
    _colors_index: int
    _symbols: List[str]
    _symbols_index: int

    def __init__(self) -> None:
        self._color_dict = {}
        self._symbol_dict = {}
        self._colors = [
            "tab:blue",
            "tab:orange",
            "tab:green",
            "tab:red",
            "tab:purple",
            "tab:brown",
            "tab:pink",
            "tab:gray",
            "tab:olive",
            "tab:cyan",
        ]
        self._colors_index = 0
        self._symbols = [
            "o",
            "v",
            "^",
            "<",
            ">",
            "1",
            "2",
            "3",
            "4",
            "8",
            "s",
            "P",
            "*",
            "D",
        ]
        self._symbols_index = 0

    def get_color(self, algorithm: str) -> str:
        if algorithm not in self._color_dict:
            self._color_dict.update(
                {algorithm: self._colors[self._colors_index % len(self._colors)]}
            )
            self._colors_index += 1
        return self._color_dict[algorithm]

    def get_symbol(self, algorithm: str) -> str:
        if algorithm not in self._symbol_dict:
            self._symbol_dict.update(
                {algorithm: self._symbols[self._symbols_index % len(self._symbols)]}
            )
            self._symbols_index += 1
        return self._symbol_dict[algorithm]
