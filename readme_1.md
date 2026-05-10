Xoá các gói java
rm -rf ~/.ivy2/cache ~/.ivy2/jars

Cài java
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"