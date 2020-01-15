import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter,OrderedDict
import seaborn as sns
import matplotlib.ticker as tick
import matplotlib.transforms as transforms


outputs = 'new_clean_join_dataset_index/clean_join_top_with_index_numerical'
# outputs = 'new_clean_join_dataset_index/clean_join_top_with_index_binary'
inputs= 'new_clean_join_dataset_index/clean_join_top_with_index_numerical/ml_top_with_index_numerical.csv'
# inpu ts= 'new_clean_join_dataset_index/clean_join_top_with_index_binary/ml_top_with_index_binary.csv'
df=pd.read_csv(inputs,engine='python',sep=",", quotechar='"',escapechar='\\', error_bad_lines=False)#format="csv", sep=",", header="true",escape='"')
print(df)

# drop missing profit row
# df = df.drop(df[df.profit ==46874848].index)

# drop missing director row
# df = df.drop(df[df.key_director ==31].index)

# drop missing runtime row
# df = df.drop(df[df.runtime ==97.66172].index)

# drop missing avg_rating row
# mode_rating = df.avg_rating.mode()[0]
# df = df.drop(df[df.avg_rating ==mode_rating].index)

# drop missing budget row
mode_budget=df.budget.mode()[0]
df = df.drop(df[df.budget ==mode_budget].index)

print(df)

# Helper Functions
def reformat_large_tick_values(tick_val, pos):
    """
    Turns large tick values (in the billions, millions and thousands) such as 4500 into 4.5K and also appropriately turns 4000 into 4K (no zero after the decimal).
    """
    if tick_val >= 1000000000:
        val = round(tick_val/1000000000, 1)
        new_tick_format = '{:}B'.format(val)
    elif tick_val >= 1000000:
        val = round(tick_val/1000000, 1)
        new_tick_format = '{:}M'.format(val)
    elif tick_val >= 1000:
        val = round(tick_val/1000, 1)
        new_tick_format = '{:}K'.format(val)
    elif tick_val < 1000:
        new_tick_format = round(tick_val, 1)
    else:
        new_tick_format = tick_val

        # make new_tick_format into a string value
        new_tick_format = str(new_tick_format)

        # code below will keep 4.5M as is but change values such as 4.0M to 4M since that zero after the decimal isn't needed
        index_of_decimal = new_tick_format.find(".")

        if index_of_decimal != -1:
            value_after_decimal = new_tick_format[index_of_decimal+1]
            if value_after_decimal == "0":
                # remove the 0 after the decimal point since it's not needed
                new_tick_format = new_tick_format[0:index_of_decimal] + new_tick_format[index_of_decimal+2:]
    return new_tick_format
def plot_runtime():
    # plot runtime histogram
    df.hist(column="runtime",bins=1000)
    plt.title("Movie Runtimes in minute")
    plt.xlabel("Different runtimes",fontsize=15)
    plt.ylabel("Numbers of Runtime",fontsize=15)
    plt.grid(b=True, which='major', color='g', linestyle='--')

    ax = plt.gca()
    # plot mean
    ax.axvline(df.runtime.mean(), color='red', linewidth=2)
    min_ylim, max_ylim = plt.ylim()
    plt.text(df.runtime.mean()*1.05, max_ylim*0.9, 'Mean: {:.2f} minutes'.format(df.runtime.mean()))
    #plot median
    ax.axvline(df.runtime.median(), color='orange', linewidth=2)
    plt.text(df.runtime.median()*0.45, max_ylim*0.85, 'Median: {:.2f} \nminutes'.format(df.runtime.median()))

    plt.xlim([0.0,250.0])
    # plt.savefig('runtime_histogram.png')
    plt.show()
def plot_avgrating():
    # plot runtime histogram
    df.hist(column="avg_rating",bins=30)
    plt.xlabel("Different avg_ratings",fontsize=15)
    plt.ylabel("Numbers of Average ratings",fontsize=15)
    plt.title("Average ratings for each movie")

    ax = plt.gca()
    #plot mean
    ax.axvline(df.avg_rating.mean(), color='red', linewidth=2)
    min_ylim, max_ylim = plt.ylim()
    plt.text(df.avg_rating.mean()*0.65, max_ylim*0.9, 'Mean: {:.2f}'.format(df.avg_rating.mean()))
    #plot median
    ax.axvline(df.avg_rating.median(), color='orange', linewidth=2)
    plt.text(df.avg_rating.median()*1.05, max_ylim*0.93, 'Median: {:.2f}'.format(df.avg_rating.median()))

    plt.grid(b=True, which='major', color='g', linestyle='--')
    plt.xlim([0.0,6.0])
    # plt.savefig('avg_rating_histogram.png')
    plt.show()
def plot_budget():
    # plot runtime histogram
    df.hist(column="budget",bins=550)
    plt.title("Movie Budgets")
    plt.xlabel("Different budgets",fontsize=15)
    plt.ylabel("Numbers of each different budgets",fontsize=15)
    plt.grid(b=True, which='major', color='g', linestyle='--')
    # plt.xlim([0.0,df['budget'].max()])
    plt.xlim([0.0,300000000])

    ax = plt.gca()
    # plot mean
    ax.axvline(df.budget.mean(), color='red', linewidth=2)
    min_ylim, max_ylim = plt.ylim()
    mean_text=df.budget.mean()
    val = round(mean_text/1000000, 1)
    new_tick_format = '{:}M'.format(val)
    plt.text(df.budget.mean()*1.05, max_ylim*0.9, "Mean: "+new_tick_format)
    #plot median
    median_text=df.budget.median()
    val = round(median_text/1000000, 1)
    new_tick_format = '{:}M'.format(val)
    ax.axvline(df.budget.median(), color='orange', linewidth=2)
    plt.text(df.budget.median()*0.45, max_ylim*0.85, 'Median: '+new_tick_format)

    # relable x-axis label
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));

    # plt.savefig('budget_histogram.png')
    plt.show()
def plot_profit():
    # plot runtime histogram
    df.hist(column="profit",bins=300)
    plt.title("Movie Profits")
    plt.xlabel("Different profits",fontsize=15)
    plt.ylabel("Numbers of each different profit",fontsize=15)
    plt.grid(b=True, which='major', color='g', linestyle='--')

    plt.xlim([df['profit'].min(),1000000000])
    ax = plt.gca()
    # plot mean
    ax.axvline(df.profit.mean(), color='red', linewidth=2)
    min_ylim, max_ylim = plt.ylim()
    mean_text=df.profit.mean()
    val = round(mean_text/1000000, 1)
    new_tick_format = '{:}M'.format(val)
    plt.text(df.profit.mean()*1.6, max_ylim*0.9, "Mean: "+new_tick_format+" (red line)")

    #plot median
    median_text=df.profit.median()
    val = round(median_text/1000000, 1)
    new_tick_format = '{:}M'.format(val)
    ax.axvline(df.profit.median(), color='orange', linewidth=2)
    plt.text(df.profit.median()*1.8, max_ylim*0.6, 'Median: '+new_tick_format+" (orange line)")

    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));

    # plt.savefig('profit_histogram.png')
    plt.show()
def plot_genres():
    # plot different genres
    genres_counter = Counter(df['genres'])
    # sort Counter
    genres_counter_sorted =OrderedDict(genres_counter.most_common())
    ### Method 1
    # df_= pd.DataFrame.from_dict(genres_count,orient='index')
    # plt.title("Movie Genres")
    # df_.plot(kind='bar',figsize=(12,12),grid=True)
    # plt.grid(b=True, which='major', color='g', linestyle='--')
    # plt.gcf().subplots_adjust(bottom=0.15)
    # plt.tight_layout()

    ### Method 2
    x_gen,y_num_gen = zip(*genres_counter_sorted.items())
    num_gen = np.arange(len(x_gen))
    plt.bar(num_gen, y_num_gen, align='center', alpha=0.5)
    plt.xticks(num_gen, x_gen)
    plt.ylabel('Number of movies')
    plt.title('Number of movies in each genre')
    plt.xticks(rotation=90)
    plt.grid(b=True, which='major', color='g', linestyle='--')
    plt.tight_layout()
    # plt.savefig('genre_histogram.png')
    plt.show()
def plot_director():
    # plot different directors
    # director_count = Counter(df['director'])
    director_count = Counter(df['key_director'])
    df_= pd.DataFrame.from_dict(director_count,orient='index')
    plt.title("Movie Directors")
    df_.plot(kind='bar',figsize=(12,12),grid=True)
    plt.show()
def plot_genre_profit():
    print("profit of missing: ",(df.profit==46874848).sum())
    genres = list(df.genres)
    profit = list(df.profit)
    plt.title("Relationship between Genres and Profits")
    plt.ylabel('Profits')
    plt.scatter(genres,profit)
    plt.grid(b=True, which='major', color='g', linestyle='--')
    plt.xticks(rotation=90)

    ax = plt.gca()
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.axhline(df.profit.mean(), color='red', linewidth=2)
    trans = transforms.blended_transform_factory(
    ax.get_yticklabels()[0].get_transform(), ax.transData)
    mean_text=df.profit.mean()
    val = round(mean_text/1000000, 1)
    new_tick_format = '{:}M'.format(val)
    ax.text(0,df.profit.mean(), new_tick_format, color="red", transform=trans,
        ha="right", va="center")
    plt.tight_layout()
    # plt.savefig('genre_profit.png')
    plt.show()

def main():
    # plot_runtime()
    # plot_avgrating()
    # plot_budget()
    # plot_profit()
    # plot_genres()
    # plot_director()
    plot_genre_profit()
if __name__ == '__main__':
    main()
