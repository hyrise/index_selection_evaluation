echo "===== Sorting imports ====="

isort --trailing-comma --line-width 90 --multi-line 3 selection/
isort --trailing-comma --line-width 90 --multi-line 3 tests/

echo ""
echo "===== Formatting via black ====="

black --line-length 90 selection/
black --line-length 90 tests
