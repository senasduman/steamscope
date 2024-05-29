from imblearn.over_sampling import SMOTE
import pandas as pd
class DataModelling:
    def __init__(self,df,number_of_target,apply_data_augmentation_bool=True): # target_number should be 3,5,9
        self.df = df
        self.number_of_target = number_of_target
        self.apply_data_augmentation_bool = apply_data_augmentation_bool

    def apply_data_augmentation(self):
        X = self.df.drop("review_text", axis = 1)
        y = self.df["review_text"]
        # SMOTE uygulama
        smote = SMOTE()
        X_resampled, y_resampled = smote.fit_resample(X, y)
        self.df = pd.concat([X_resampled, y_resampled], axis=1)
    
    def map_review_texts(self):
        if self.number_of_target == 3:
            # 'Positive' kelimesini içeren tüm değerleri 'Positive' olarak güncelleme
            self.df['review_text'] = self.df['review_text'].apply(lambda x: 'Positive' if 'Positive' in x else x)

            # 'Negative' kelimesini içeren tüm değerleri 'Negative' olarak güncelleme
            self.df['review_text'] = self.df['review_text'].apply(lambda x: 'Negative' if 'Negative' in x else x)

            mapping = {
                "Positive" : 2,
                "Mixed"    : 1,
                "Negative" : 0,
            }

            # Haritalama işlemi uygulama
            self.df['review_text'] = self.df['review_text'].map(mapping) 
        
        if self.number_of_target == 5:
            self.df['review_text'] = self.df['review_text'].replace('Overwhelmingly Positive', 'Very Positive')
            self.df['review_text'] = self.df['review_text'].replace('Mostly Positive', 'Very Positive')

            self.df['review_text'] = self.df['review_text'].replace('Mostly Negative', 'Very Negative')
            self.df['review_text'] = self.df['review_text'].replace('Overwhelmingly Negative', 'Very Negative')

            mapping = {
                "Very Positive" : 4,
                "Positive" : 3,
                "Mixed"    : 2,
                "Negative" : 1,
                "Very Negative" : 0,
            }

            # Haritalama işlemi uygulama
            self.df['review_text'] = self.df['review_text'].map(mapping)

        if self.number_of_target == 9:
            mapping = {
                'Overwhelmingly Positive': 8,
                'Very Positive': 7,
                'Mostly Positive': 6,
                'Positive': 5,
                'Mixed': 4,
                'Negative': 3,
                'Mostly Negative': 2,
                'Very Negative': 1,
                'Overwhelmingly Negative': 0
            }

            # Haritalama işlemi uygulama
            self.df['review_text'] = self.df['review_text'].map(mapping)
        
    def run_modelling(self):
        if self.apply_data_augmentation_bool:
            self.apply_data_augmentation()
        
        self.map_review_texts()
        return self.df